package xmplus

import ( 
    "encoding/json"
)

type serverConfig struct {
	server           `json:"server"`
	version string   `json:"version"`
	Relay   bool 	 `json:"relay"`
	relay_server     `json:"relay_server"`
	Routes []route   `json:"rules"`
}

type server struct {
	Address     string 	 `json:"address"`
	Certmode    string 	 `json:"certmode"`
	Cipher      string 	 `json:"cipher"`
	IP          string   `json:"ip"`
	Port        int      `json:"listeningport"`
	Listenip    string   `json:"listenip"`
	NetworkSettings struct {
	    ProxyProtocol bool 	           `json:"acceptProxyProtocol"`
		Path          string           `json:"path"`
		Host          string           `json:"host"`
		QuicSecurity  string           `json:"security"`
		Quickey       string           `json:"key"`
		Headers       *json.RawMessage `json:"headers"`
		ServiceName   string           `json:"serviceName"`
		Authority     string           `json:"authority"`
		Header        *json.RawMessage `json:"header"`
		Transport     string           `json:"transport"`
		Seed          string           `json:"seed"`
		Congestion    bool 	           `json:"congestion"`
		Flow          string           `json:"flow"`
		MaxConcurrentUploads int       `json:"maxconcurrentuploads"`
		MaxUploadSize  int             `json:"maxuploadsize"`
	} `json:"networkSettings"`
	Security    string `json:"security"`
	SecuritySettings  struct {
	    AllowInsecure bool 	    `json:"allowInsecure"`
		Fingerprint   string    `json:"fingerprint"`
		RejectUnknownSni bool   `json:"rejectUnknownSni"`
		ServerName    string    `json:"serverName"`
		Dest          string    `json:"dest"`
		Show          bool      `json:"show"`  
		PrivateKey    string    `json:"privatekey"`
		MinClientVer  string    `json:"minclientver"`
		MaxClientVer  string    `json:"maxclientver"`
		MaxTimeDiff   int       `json:"maxtimediff"`
		ProxyProtocol int       `json:"proxyprotocol"`
		ServerNames   []string  `json:"serverNames"`
		ShortIds      []string  `json:"shortids"`
	} `json:"securitySettings"`	
	Relayid     int        `json:"relayid"`
	SendThrough string     `json:"sendthrough"`
	ServerKey   string     `json:"server_key"`
	Sniffing    bool 	   `json:"sniffing"`
	Speedlimit  int        `json:"speedlimit"`
	Type        string     `json:"type"`
}

type relay_server struct {
	RId          int        `json:"id"`
	RAddress     string 	`json:"address"`
	RServerid    int 	    `json:"serverid"`
	RCipher      string 	`json:"cipher"`
	RIP          string     `json:"ip"`
	RPort        int        `json:"listeningport"`
	RListenip    string     `json:"listenip"`
	RNetworkSettings struct {
	    ProxyProtocol bool 	           `json:"acceptProxyProtocol"`
		Path          string           `json:"path"`
		Host          string           `json:"host"`
		QuicSecurity  string           `json:"security"`
		Quickey       string           `json:"key"`
		Headers       *json.RawMessage `json:"headers"`
		ServiceName   string           `json:"serviceName"`
		Authority     string           `json:"authority"`
		Header        *json.RawMessage `json:"header"`
		Transport     string           `json:"transport"`
		Seed          string           `json:"seed"`
		Congestion    bool 	           `json:"congestion"`
		Flow          string           `json:"flow"`
		MaxConcurrentUploads int       `json:"maxconcurrentuploads"`
		MaxUploadSize  int             `json:"maxuploadsize"`
	} `json:"networkSettings"`
	RSecurity string `json:"security"`
	RSecuritySettings struct {
		Fingerprint   string    `json:"fingerprint"`
		ServerName    string    `json:"serverName"`
		Show          bool      `json:"show"`  
		PublicKey     string    `json:"publickey"`
		ShortId       string    `json:"shortid"`
		SpiderX       string    `json:"spiderx"`
	} `json:"securitySettings"`	
	RSendThrough string   `json:"sendthrough"`
	RServerKey  string    `json:"server_key"`
	RSniffing  bool 	  `json:"sniffing"`
	RSpeedlimit  int      `json:"speedlimit"`
	RType     string      `json:"type"`
}

type route struct {
	Id       int      `json:"id"`
	Regex    string   `json:"regex"`
}

// ServiceResponse is the common response
type ServiceResponse struct {
	Data json.RawMessage `json:"services"`
}

type Service struct {
	Id         int    `json:"id"`
	Uuid       string `json:"uuid"`
	Email      string `json:"email"`
	Passwd     string `json:"passwd"`
	Speedlimit int    `json:"speedlimit"`
	Iplimit    int    `json:"iplimit"`
	Ipcount    int    `json:"ipcount"`
}

// Response is the common response
type Response struct {
	Ret  uint            `json:"ret"`
	Data json.RawMessage `json:"data"`
}

// PostData is the data structure of post data
type PostData struct {
	Data interface{} `json:"data"`
}

// OnlineUser is the data structure of online user
type OnlineIP struct {
	UID int    `json:"serviceid"`
	IP  string `json:"ip"`
}

// UserTraffic is the data structure of traffic
type ServiceTraffic struct {
	UID      int   `json:"serviceid"`
	Upload   int64 `json:"u"`
	Download int64 `json:"d"`
}

type IllegalItem struct {
	ID  int `json:"list_id"`
	UID int `json:"serviceid"`
}
