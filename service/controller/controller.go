package controller

import (
	"fmt"
	"log"
	"reflect"
	"time"
	"strings"
	
	"github.com/xmplusdev/xmcore/common/protocol"
	"github.com/xmplusdev/xmcore/common/task"
	"github.com/xmplusdev/xmcore/core"
	"github.com/xmplusdev/xmcore/features/inbound"
	"github.com/xmplusdev/xmcore/features/outbound"
	"github.com/xmplusdev/xmcore/features/routing"
	"github.com/xmplusdev/xmcore/features/stats"
	"github.com/xmplusdev/xmcore/app/router"
	"github.com/XMPlusDev/XMPlus-Relay/api"
	"github.com/XMPlusDev/XMPlus-Relay/app/xdispatcher"
	"github.com/XMPlusDev/XMPlus-Relay/utility/mylego"
	C "github.com/sagernet/sing/common"
	"github.com/sagernet/sing-shadowsocks/shadowaead_2022"
	"github.com/xmplusdev/xmcore/infra/conf"
)

type Controller struct {
	server       *core.Instance
	config       *Config
	clientInfo   api.ClientInfo
	apiClient    api.API
	nodeInfo     *api.NodeInfo
	Tag          string
	serviceList  *[]api.ServiceInfo
	tasks        []periodicTask
	ibm          inbound.Manager
	obm          outbound.Manager
	stm          stats.Manager
	dispatcher   *xdispatcher.DefaultDispatcher
	startAt      time.Time
	rdispatcher  *router.Router
	RelayTag     string
	Relay        bool
	relaynodeInfo *api.RelayNodeInfo
}

type periodicTask struct {
	tag string
	*task.Periodic
}

// New return a Controller service with default parameters.
func New(server *core.Instance, api api.API, config *Config) *Controller {
	controller := &Controller{
		server:     server,
		config:     config,
		apiClient:  api,
		ibm:        server.GetFeature(inbound.ManagerType()).(inbound.Manager),
		obm:        server.GetFeature(outbound.ManagerType()).(outbound.Manager),
		stm:        server.GetFeature(stats.ManagerType()).(stats.Manager),
		dispatcher: server.GetFeature(routing.DispatcherType()).(*xdispatcher.DefaultDispatcher),
		rdispatcher: server.GetFeature(routing.RouterType()).(*router.Router),
		startAt:    time.Now(),
	}

	return controller
}

// Start implement the Start() function of the service interface
func (c *Controller) Start() error {
	c.clientInfo = c.apiClient.Describe()
	
	// First fetch Node Info
	newNodeInfo, err := c.apiClient.GetNodeInfo()
	if err != nil {
		return err
	}
	c.nodeInfo = newNodeInfo
	c.Tag = c.buildNodeTag()

	// Update service
	serviceInfo, err := c.apiClient.GetServiceList()
	if err != nil {
		return err
	}

	// sync controller serviceList
	c.serviceList = serviceInfo

	c.Relay = false
	// Add new Relay	tag
	if c.nodeInfo.Relay {
		newRelayNodeInfo, err := c.apiClient.GetRelayNodeInfo()
		if err != nil {
			log.Panic(err)
			return nil
		}	
		c.relaynodeInfo = newRelayNodeInfo
		c.RelayTag = c.buildRNodeTag()
		
		//log.Printf("%s Taking a Detour Route [%s] For Services", c.logPrefix(), c.RelayTag)
		err = c.addNewRelayTag(newRelayNodeInfo, serviceInfo)
		if err != nil {
			log.Panic(err)
			return err
		}
		c.Relay = true
	}
	
	// Add new tag
	err = c.addNewTag(newNodeInfo)
	if err != nil {
		log.Panic(err)
		return err
	}

	err = c.addNewService(serviceInfo, newNodeInfo)
	if err != nil {
		return err
	}

	// Add Limiter
	if err := c.AddInboundLimiter(c.Tag, newNodeInfo.SpeedLimit, serviceInfo, c.config.IPLimit); err != nil {
		log.Print(err)
	}

	// Add Rule Manager

	if ruleList, err := c.apiClient.GetNodeRule(); err != nil {
		log.Printf("Get rule list filed: %s", err)
	} else if len(*ruleList) > 0 {
		if err := c.UpdateRule(c.Tag, *ruleList); err != nil {
			log.Print(err)
		}
	}

	// Add periodic tasks
	c.tasks = append(c.tasks,
		periodicTask{
			tag: "node",
			Periodic: &task.Periodic{
				Interval: time.Duration(60) * time.Second,
				Execute:  c.nodeInfoMonitor,
			}},
		periodicTask{
			tag: "services",
			Periodic: &task.Periodic{
				Interval: time.Duration(60) * time.Second,
				Execute:  c.userInfoMonitor,
			}},
	)

	// Check cert service in need
	if c.nodeInfo.TLSType == "tls"  && c.nodeInfo.CertMode != "none" {
		c.tasks = append(c.tasks, periodicTask{
			tag: "cert renew",
			Periodic: &task.Periodic{
				Interval: time.Duration(60) * time.Second * 60,
				Execute:  c.certMonitor,
			}})
	}

	// Start periodic tasks
	for i := range c.tasks {
		log.Printf("%s Task Scheduler for %s started", c.logPrefix(), c.tasks[i].tag)
		go c.tasks[i].Start()
	}

	return nil
}

// Close implement the Close() function of the service interface
func (c *Controller) Close() error {
	for i := range c.tasks {
		if c.tasks[i].Periodic != nil {
			if err := c.tasks[i].Periodic.Close(); err != nil {
				log.Panicf("%s Task Scheduler for  %s failed to close: %s", c.logPrefix(), c.tasks[i].tag, err)
			}
		}
	}

	return nil
}

func (c *Controller) nodeInfoMonitor() (err error) {
	// delay to start
	if time.Since(c.startAt) < time.Duration(60)*time.Second {
		return nil
	}	
	
	// First fetch Node Info
	var nodeInfoChanged = true
	newNodeInfo, err := c.apiClient.GetNodeInfo()
	if err != nil {
		if err.Error() == api.NodeNotModified {
			nodeInfoChanged = false
			newNodeInfo = c.nodeInfo
		} else {
			log.Print(err)
			return nil
		}
	}	

	// Update User
	
	var serviceChanged = true
	
	newServiceInfo, err := c.apiClient.GetServiceList()
	
	if err != nil {
		if nodeInfoChanged {
			if err.Error() == api.ServiceNotModified {
				serviceChanged = true
			} else {
				log.Print(err)
				return nil
			}
		}else{
			if err.Error() == api.ServiceNotModified  {
				serviceChanged = false
				newServiceInfo = c.serviceList
			} else {
				log.Print(err)
				return nil
			}
		}
		
	}
	
	var updateRelay = false	
	
	if serviceChanged ||  nodeInfoChanged {
		updateRelay = true
	}
	
	if updateRelay {
		c.removeRules(c.Tag, c.serviceList)
	}
	
	// If nodeInfo changed
	if nodeInfoChanged {
		if !reflect.DeepEqual(c.nodeInfo, newNodeInfo) {
			// Remove old tag
			oldTag := c.Tag
			err := c.removeOldTag(oldTag)
			if err != nil {
				log.Print(err)
				return nil
			}
			if c.nodeInfo.NodeType == "Shadowsocks-Plugin" {
				err = c.removeOldTag(fmt.Sprintf("dokodemo-door_%s+1", c.Tag))
			}
			if err != nil {
				log.Print(err)
				return nil
			}
			
			// Add new tag
			c.nodeInfo = newNodeInfo
			c.Tag = c.buildNodeTag()
			err = c.addNewTag(newNodeInfo)
			if err != nil {
				log.Print(err)
				return nil
			}
			nodeInfoChanged = true
			// Remove Old limiter
			if err = c.DeleteInboundLimiter(oldTag); err != nil {
				log.Print(err)
				return nil
			}
		} else {
			nodeInfoChanged = false
		}
	}

	// Remove relay tag
	if c.Relay && updateRelay {
		err := c.removeRelayTag(c.RelayTag, c.serviceList)
		if err != nil {
			return err
		}
		c.Relay = false
	}
	
	// Update new Relay tag
	if c.nodeInfo.Relay && updateRelay {
		newRelayNodeInfo, err := c.apiClient.GetRelayNodeInfo()
		if err != nil {
			log.Panic(err)
			return nil
		}	
		c.relaynodeInfo = newRelayNodeInfo
		c.RelayTag = c.buildRNodeTag()
		
		//log.Printf("%s Reload Detour Route [%s] For Services", c.logPrefix(), c.RelayTag)
		
		err = c.addNewRelayTag(newRelayNodeInfo, newServiceInfo)
		if err != nil {
			log.Panic(err)
			return err
		}
		c.Relay = true
	}
	
	// Check Rule
	
	if ruleList, err := c.apiClient.GetNodeRule(); err != nil {
		if err.Error() != api.RuleNotModified {
			log.Printf("Get rule list filed: %s", err)
		}
	} else if len(*ruleList) > 0 {
		if err := c.UpdateRule(c.Tag, *ruleList); err != nil {
			log.Print(err)
		}
	}
	

	if nodeInfoChanged {
		err = c.addNewService(newServiceInfo, newNodeInfo)
		if err != nil {
			log.Print(err)
			return nil
		}

		// Add Limiter
		if err := c.AddInboundLimiter(c.Tag, newNodeInfo.SpeedLimit, newServiceInfo, c.config.IPLimit); err != nil {
			log.Print(err)
			return nil
		}	
	} else {
		var deleted, added []api.ServiceInfo
		if serviceChanged {
			deleted, added = compareServiceList(c.serviceList, newServiceInfo)
			if len(deleted) > 0 {
				deletedEmail := make([]string, len(deleted))
				for i, u := range deleted {
					deletedEmail[i] = fmt.Sprintf("%s|%s|%d", c.Tag, u.Email, u.UID)
				}
				err := c.removeServices(deletedEmail, c.Tag)
				if err != nil {
					log.Print(err)
				}
				log.Printf("%s %d Service(s) deleted", c.logPrefix(), len(deleted))
			}
			if len(added) > 0 {
				err = c.addNewService(&added, c.nodeInfo)
				if err != nil {
					log.Print(err)
				}
				// Update Limiter
				if err := c.UpdateInboundLimiter(c.Tag, &added); err != nil {
					log.Print(err)
				}
			}
		}	
	}
	c.serviceList = newServiceInfo
	return nil
}

func (c *Controller) removeRelayTag(tag string, serviceInfo *[]api.ServiceInfo) (err error) {
	for _, service := range *serviceInfo {
		err = c.removeOutbound(fmt.Sprintf("%s_%d", tag, service.UID))
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Controller) removeRules(tag string, serviceInfo *[]api.ServiceInfo){
	for _, service := range *serviceInfo {
		c.RemoveUserRule([]string{c.buildUserTag(&service)})			
	}	
}

func (c *Controller) addNewRelayTag(newRelayNodeInfo *api.RelayNodeInfo, serviceInfo *[]api.ServiceInfo) (err error) {
	if newRelayNodeInfo.NodeType != "Shadowsocks-Plugin" {
		for _, service := range *serviceInfo {
			var Key string			
			if C.Contains(shadowaead_2022.List, strings.ToLower(newRelayNodeInfo.CypherMethod)) {
				userKey, err := c.checkShadowsocksPassword(service.Passwd, newRelayNodeInfo.CypherMethod)
				if err != nil {
					newError(fmt.Errorf("[UID: %d] %s", service.UUID, err)).AtError()
					continue
				}
				Key = fmt.Sprintf("%s:%s", newRelayNodeInfo.ServerKey, userKey)
			} else {
				Key = service.Passwd
			}
			RelayTagConfig, err := OutboundRelayBuilder(newRelayNodeInfo, c.RelayTag, service.UUID, service.Email, Key, service.UID)
			if err != nil {
				return err
			}
			
			err = c.addOutbound(RelayTagConfig)
			if err != nil {
				return err
			}
			c.AddUserRule(fmt.Sprintf("%s_%d", c.RelayTag, service.UID), []string{c.buildUserTag(&service)})		
		}
	}
	return nil
}


func (c *Controller) removeOldTag(oldTag string) (err error) {
	err = c.removeInbound(oldTag)
	if err != nil {
		return err
	}
	err = c.removeOutbound(oldTag)
	if err != nil {
		return err
	}
	return nil
}

func (c *Controller) addNewTag(newNodeInfo *api.NodeInfo) (err error) {
	if newNodeInfo.NodeType != "Shadowsocks-Plugin" {
		inboundConfig, err := InboundBuilder(c.config, newNodeInfo, c.Tag)
		if err != nil {
			return err
		}
		err = c.addInbound(inboundConfig)
		if err != nil {
			return err
		}
		outBoundConfig, err := OutboundBuilder(c.config, newNodeInfo, c.Tag)
		if err != nil {
			return err
		}
		err = c.addOutbound(outBoundConfig)
		if err != nil {
			return err
		}
	} else {
		return c.addInboundForSSPlugin(*newNodeInfo)
	}
	return nil
}

func (c *Controller) addInboundForSSPlugin(newNodeInfo api.NodeInfo) (err error) {
	// Shadowsocks-Plugin require a separate inbound for other TransportProtocol likes: ws, grpc
	fakeNodeInfo := newNodeInfo
	fakeNodeInfo.Transport = "tcp"
	// Add a regular Shadowsocks inbound and outbound
	inboundConfig, err := InboundBuilder(c.config, &fakeNodeInfo, c.Tag)
	if err != nil {
		return err
	}
	err = c.addInbound(inboundConfig)
	if err != nil {

		return err
	}
	outBoundConfig, err := OutboundBuilder(c.config, &fakeNodeInfo, c.Tag)
	if err != nil {

		return err
	}
	err = c.addOutbound(outBoundConfig)
	if err != nil {

		return err
	}
	// Add an inbound for upper streaming protocol
	fakeNodeInfo = newNodeInfo
	fakeNodeInfo.Port++
	fakeNodeInfo.NodeType = "dokodemo-door"
	dokodemoTag := fmt.Sprintf("dokodemo-door_%s+1", c.Tag)
	inboundConfig, err = InboundBuilder(c.config, &fakeNodeInfo, dokodemoTag)
	if err != nil {
		return err
	}
	err = c.addInbound(inboundConfig)
	if err != nil {

		return err
	}
	outBoundConfig, err = OutboundBuilder(c.config, &fakeNodeInfo, dokodemoTag)
	if err != nil {

		return err
	}
	err = c.addOutbound(outBoundConfig)
	if err != nil {

		return err
	}
	return nil
}

func (c *Controller) addNewService(serviceInfo *[]api.ServiceInfo, nodeInfo *api.NodeInfo) (err error) {
	services := make([]*protocol.User, 0)
	switch nodeInfo.NodeType {
	case "Vless":
		services = c.buildVlessUser(serviceInfo, nodeInfo.Flow)
	case "Vmess":
		services = c.buildVmessUser(serviceInfo)	
	case "Trojan":
		services = c.buildTrojanUser(serviceInfo)
	case "Shadowsocks":
		services = c.buildSSUser(serviceInfo, nodeInfo.CypherMethod)
	case "Shadowsocks-Plugin":
		services = c.buildSSPluginUser(serviceInfo, nodeInfo.CypherMethod)	
	default:
		return fmt.Errorf("unsupported node type: %s", nodeInfo.NodeType)
	}

	err = c.addServices(services, c.Tag)
	if err != nil {
		return err
	}
	log.Printf("%s %d New Service(s) Added", c.logPrefix(), len(*serviceInfo))
	return nil
}

func compareServiceList(old, new *[]api.ServiceInfo) (deleted, added []api.ServiceInfo) {
	mSrc := make(map[api.ServiceInfo]byte) 
	mAll := make(map[api.ServiceInfo]byte) 

	var set []api.ServiceInfo 

	for _, v := range *old {
		mSrc[v] = 0
		mAll[v] = 0
	}

	for _, v := range *new {
		l := len(mAll)
		mAll[v] = 1
		if l != len(mAll) {
			l = len(mAll)
		} else { 
			set = append(set, v)
		}
	}
	
	for _, v := range set {
		delete(mAll, v)
	}
	
	for v := range mAll {
		_, exist := mSrc[v]
		if exist {
			deleted = append(deleted, v)
		} else {
			added = append(added, v)
		}
	}

	return deleted, added
}

func (c *Controller) userInfoMonitor() (err error) {
	// Get Service traffic
	var serviceTraffic []api.ServiceTraffic
	var upCounterList []stats.Counter
	var downCounterList []stats.Counter

	for _, service := range *c.serviceList {
		up, down, upCounter, downCounter := c.getTraffic(c.buildUserTag(&service))
		if up > 0 || down > 0 {
			serviceTraffic = append(serviceTraffic, api.ServiceTraffic{
				UID:      service.UID,
				Email:    service.Email,
				Upload:   up,
				Download: down})

			if upCounter != nil {
				upCounterList = append(upCounterList, upCounter)
			}
			if downCounter != nil {
				downCounterList = append(downCounterList, downCounter)
			}
		}
	}

	if len(serviceTraffic) > 0 {
		var err error // Define an empty error

		err = c.apiClient.ReportServiceTraffic(&serviceTraffic)
		// If report traffic error, not clear the traffic
		if err != nil {
			log.Print(err)
		} else {
			c.resetTraffic(&upCounterList, &downCounterList)
		}
	}

	// Report Online info
	if onlineDevice, err := c.GetOnlineDevice(c.Tag); err != nil {
		log.Print(err)
	} else if len(*onlineDevice) > 0 {
		if err = c.apiClient.ReportNodeOnlineIPs(onlineDevice); err != nil {
			log.Print(err)
		} else {
			log.Printf("%s Report %d online IPs", c.logPrefix(), len(*onlineDevice))
		}
	}
	
	// Report Illegal user
	if detectResult, err := c.GetDetectResult(c.Tag); err != nil {
		log.Print(err)
	} else if len(*detectResult) > 0 {
		log.Printf("%s blocked %d access by detection rules", c.logPrefix(), len(*detectResult))
	}

	return nil
}

func (c *Controller) buildNodeTag() string {
	return fmt.Sprintf("%s_%d_%d", c.nodeInfo.NodeType, c.nodeInfo.Port, c.nodeInfo.NodeID)
}

func (c *Controller) buildRNodeTag() string {
	return fmt.Sprintf("Relay_%d_%s_%d_%d", c.nodeInfo.NodeID, c.relaynodeInfo.NodeType, c.relaynodeInfo.Port, c.relaynodeInfo.NodeID)
}

func (c *Controller) logPrefix() string {
	transportProtocol := conf.TransportProtocol(c.nodeInfo.Transport)
	networkType, err := transportProtocol.Build()
	if err != nil {
		return fmt.Sprintf("[%s] %s(NodeID=%d)", c.clientInfo.APIHost, c.nodeInfo.NodeType, c.nodeInfo.NodeID)
	}
	
	return fmt.Sprintf("[%s] %s(NodeID=%d) [Transport=%s]", c.clientInfo.APIHost, c.nodeInfo.NodeType, c.nodeInfo.NodeID, networkType)
}

// Check Cert
func (c *Controller) certMonitor() error {
	switch c.nodeInfo.CertMode {
	case "dns", "http":
		lego, err := mylego.New(c.config.CertConfig)
		if err != nil {
			log.Print(err)
		}
		_, _, _, err = lego.RenewCert(c.nodeInfo.CertMode, c.nodeInfo.CertDomain)
		if err != nil {
			log.Print(err)
		}
	}
	
	return nil
}
