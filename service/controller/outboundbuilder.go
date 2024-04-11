package controller

import (
	"encoding/json"
	"fmt"

	"github.com/xmplusdev/xmcore/common/net"
	"github.com/xmplusdev/xmcore/core"
	"github.com/xmplusdev/xmcore/infra/conf"
	"github.com/XMPlusDev/XMPlus-Relay/api"
)

// OutboundBuilder build freedom outbound config for addOutbound
func OutboundBuilder(config *Config, nodeInfo *api.NodeInfo, tag string) (*core.OutboundHandlerConfig, error) {
	outboundDetourConfig := &conf.OutboundDetourConfig{}
	outboundDetourConfig.Protocol = "freedom"
	outboundDetourConfig.Tag = tag

	// Build Send IP address
	if nodeInfo.SendIP != "" {
		outboundDetourConfig.SendThrough = &nodeInfo.SendIP
	}

	// Freedom Protocol setting
	var domainStrategy = "Asis"
	if config.EnableDNS {
		if config.DNSStrategy != "" {
			domainStrategy = config.DNSStrategy
		} else {
			domainStrategy = "Asis"
		}
	}
	proxySetting := &conf.FreedomConfig{
		DomainStrategy: domainStrategy,
	}
	
	if config.EnableFragment {
		fragmentConfigs, err := buildFragment(config.FragmentConfigs)
		if err == nil {
			proxySetting.Fragment = fragmentConfigs
		}else {
			return nil, err
		}	
	}
	
	// Used for Shadowsocks-Plugin
	if nodeInfo.NodeType == "dokodemo-door" {
		proxySetting.Redirect = fmt.Sprintf("127.0.0.1:%d", nodeInfo.Port-1)
	}
	var setting json.RawMessage
	setting, err := json.Marshal(proxySetting)
	if err != nil {
		return nil, fmt.Errorf("marshal proxy %s config fialed: %s", nodeInfo.NodeType, err)
	}
	outboundDetourConfig.Settings = &setting
	return outboundDetourConfig.Build()
}

func buildFragment(fragmentConfigs *FragmentConfig) (*conf.Fragment, error) {
	if fragmentConfigs == nil {
		return nil, fmt.Errorf("you must provide FragmentConfig")
	}

	return &conf.Fragment{
		Packets: fragmentConfigs.Packets,
		Length: fragmentConfigs.Length,
		Interval: fragmentConfigs.Interval,
	}, nil
}