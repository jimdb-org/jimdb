// Copyright 2019 The JIMDB Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package entity

import (
	"bytes"
	"fmt"
	"github.com/jimdb-org/jimdb/master/utils/log"
	"net"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync"

	"github.com/BurntSushi/toml"
	"github.com/pkg/errors"
	"github.com/spf13/cast"
	"go.etcd.io/etcd/embed"
)

var single *Config

func Conf() *Config {
	return single
}

var (
	versionOnce  sync.Once
	buildVersion = "0.0"
	buildTime    = "0"
	commitID     = "xxxxx"
)

func SetConfigVersion(bv, bt, ci string) {
	versionOnce.Do(func() {
		buildVersion = bv
		buildTime = bt
		commitID = ci
	})
}

func GetBuildVersion() string {
	return buildVersion
}
func GetBuildTime() string {
	return buildTime
}
func GetCommitID() string {
	return commitID
}

const (
	LocalSingleAddr = "127.0.0.1"
	LocalCastAddr   = "0.0.0.0"
)

type Config struct {
	Global  *GlobalCfg `toml:"global,omitempty" json:"global"`
	Masters Masters    `toml:"masters,omitempty" json:"masters"`
}

type GlobalCfg struct {
	Name           string  `toml:"name,omitempty" json:"name"`
	Log            string  `toml:"log,omitempty" json:"log"`
	Data           string  `toml:"data,omitempty" json:"data"`
	Level          string  `toml:"level,omitempty" json:"level"`
	Signkey        string  `toml:"signkey,omitempty" json:"signkey"`
	ClusterID      uint64  `toml:"cluster_id,omitempty" json:"cluster_id"`
	ReplicaNum     int     `toml:"replica_num,omitempty" json:"replica_num"`
	ScheduleSecond int     `toml:"schedule_second,omitempty" json:"schedule_second"`
	PeerDownSecond int     `toml:"peer_down_second,omitempty" json:"peer_down_second"`
	MemoryRatio    float64 `toml:"memory_ratio,omitempty" json:"memory_ratio"`

	MonitorEnable  bool    `toml:"monitor_enable,omitempty" json:"monitor_enable"`
	Monitor        string  `toml:"monitor,omitempty" json:"monitor"`
	PushGateway    string  `toml:"push_gateway,omitempty" json:"push_gateway"`
	PushInterval   int     `toml:"push_interval,omitempty" json:"push_interval"`
}

type Masters []*MasterCfg

//new client use this function to get client urls
func (ms Masters) ClientAddress() []string {
	addrs := make([]string, len(ms))
	for i, m := range ms {
		addrs[i] = m.Address + ":" + cast.ToString(ms[i].EtcdClientPort)
	}
	return addrs
}

func (ms Masters) Self() *MasterCfg {
	for _, m := range ms {
		if m.Self {
			return m
		}
	}
	return nil
}

type MasterCfg struct {
	Name           string `toml:"name,omitempty" json:"name"`
	Address        string `toml:"address,omitempty" json:"address"`
	ApiPort        uint16 `toml:"api_port,omitempty" json:"api_port"`
	EtcdPort       uint16 `toml:"etcd_port,omitempty" json:"etcd_port"`
	EtcdPeerPort   uint16 `toml:"etcd_peer_port,omitempty" json:"etcd_peer_port"`
	EtcdClientPort uint16 `toml:"etcd_client_port,omitempty" json:"etcd_client_port"`
	Self           bool   `json:"-"`
	PprofPort      uint16 `toml:"pprof_port,omitempty" json:"pprof_port"`
}

func (m *MasterCfg) ApiUrl() string {
	return "http://" + m.Address + ":" + cast.ToString(m.ApiPort)
}

//GetEmbed will get or generate the etcd configuration
func (config *Config) GetEmbed() (*embed.Config, error) {
	masterCfg := config.Masters.Self()

	if masterCfg == nil {
		return nil, fmt.Errorf("not found master config by this machine, please ip , domain , or url config")
	}

	cfg := embed.NewConfig()
	cfg.Name = masterCfg.Name
	cfg.Dir = config.Global.Data
	cfg.WalDir = ""
	cfg.ClusterState = embed.ClusterStateFlagNew
	cfg.EnablePprof = false
	cfg.PreVote = true
	cfg.StrictReconfigCheck = true
	cfg.TickMs = uint(100)
	cfg.ElectionMs = uint(3000)
	cfg.AutoCompactionMode = "periodic"
	cfg.AutoCompactionRetention = "1"
	cfg.MaxRequestBytes = 33554432
	cfg.QuotaBackendBytes = 8589934592
	cfg.InitialClusterToken = config.Global.Signkey

	//set init url
	buf := bytes.Buffer{}
	for _, m := range config.Masters {
		if buf.Len() > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(m.Name)
		buf.WriteString("=http://")
		buf.WriteString(m.Address)
		buf.WriteString(":")
		buf.WriteString(cast.ToString(masterCfg.EtcdPeerPort))
	}
	cfg.InitialCluster = buf.String()

	if urlAddr, err := url.Parse("http://" + masterCfg.Address + ":" + cast.ToString(masterCfg.EtcdPeerPort)); err != nil {
		return nil, err
	} else {
		cfg.LPUrls = []url.URL{*urlAddr}
		cfg.APUrls = []url.URL{*urlAddr}
	}

	if urlAddr, err := url.Parse("http://" + masterCfg.Address + ":" + cast.ToString(masterCfg.EtcdClientPort)); err != nil {
		return nil, err
	} else {
		cfg.ACUrls = []url.URL{*urlAddr}
		cfg.LCUrls = []url.URL{*urlAddr}
	}

	return cfg, nil
}

func InitConfig(path string) {
	single = &Config{}
	LoadConfig(single, path)
}

func LoadConfig(conf *Config, path string) {
	if len(path) == 0 {
		log.Error("configPath file is empty!")
		os.Exit(-1)
	}
	if _, err := toml.DecodeFile(path, conf); err != nil {
		log.Error("decode:[%s] failed, err:[%s]", path, err.Error())
		os.Exit(-1)
	}
}

//CurrentByMasterNameDomainIp find this machine domain.The main purpose of this function is to find the master from from multiple masters and set it‘s Field:self to true.
//The only criterion for judging is: Is the IP address the same with one of the masters?
func (config *Config) CurrentByMasterNameDomainIp(masterName string) error {

	//find local all ip
	addrMap := config.addrMap()

	var found bool
	if config.Global.Name != "" && masterName != "" && config.Global.Name != masterName {
		return errors.New("server name confusion")
	}
	for _, m := range config.Masters {
		if m.Name == masterName {
			m.Self = true
			found = true
			if masterName != "" {
				config.Global.Name = masterName
			}
		} else if addrMap[m.Address] {
			log.Info("found local master successfully :master's name:[%s] master's ip:[%s] and local master's name:[%s]", m.Name, m.Address, masterName)
			m.Self = true
			found = true
			if masterName != "" {
				config.Global.Name = masterName
			}
		} else {
			log.Info("find local master failed:master's name:[%s] master's ip:[%s] and local master's name:[%s]", m.Name, m.Address, masterName)
		}
	}

	if !found {
		return errors.New("None of the masters has the same ip address as current local master server's ip")
	}
    if config.Global.Name == "" {
    	return errors.New("server must has a name")
    }
	return nil
}

func (config *Config) addrMap() map[string]bool {
	addrMap := map[string]bool{LocalSingleAddr: true, LocalCastAddr: true}
	ifaces, err := net.Interfaces()
	if err != nil {
		panic(err)
	}
	for _, i := range ifaces {
		addrs, _ := i.Addrs()
		for _, addr := range addrs {
			match, _ := regexp.MatchString(`^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+/[0-9]+$`, addr.String())
			if !match {
				continue
			}
			slit := strings.Split(addr.String(), "/")
			addrMap[slit[0]] = true
		}
	}
	return addrMap
}

func (config *Config) Validate() error {

	masterNum := 0
	for _, m := range config.Masters {
		if m.Self {
			masterNum++
		}
	}

	if masterNum > 1 {
		return fmt.Errorf("in one machine has two masters")
	}
	if config.Global.MonitorEnable {
		if config.Global.Name == "" {
			config.Global.Name = "jimdb"
		}
		if config.Global.PushInterval == 0 {
			// default push interval 5s
			config.Global.PushInterval = 5
		}
		if config.Global.PushGateway == "" {
			return fmt.Errorf("invalid push gateway url")
		}
		if config.Global.Monitor == "" {
			config.Global.Monitor = "prometheus"
		}
		switch config.Global.Monitor {
		case "prometheus", "baudtime":
		default:
			return fmt.Errorf("invalid monitor %s", config.Global.Monitor)
		}
	}

	return nil
}

func (config *Config) validatePath() error {
	if err := os.MkdirAll(config.Global.Log, os.ModePerm); err != nil {
		return err
	}

	if err := os.MkdirAll(config.Global.Data, os.ModePerm); err != nil {
		return err
	}

	return nil
}
