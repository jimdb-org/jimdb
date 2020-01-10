// Copyright 2018 The JimDB Authors.
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
	"github.com/jimdb-org/jimdb/jimkv/master/utils/monitoring"
	"time"
	"github.com/jimdb-org/jimdb/jimkv/master/utils/log"
	"fmt"
)

var monitor monitoring.Monitor

func Monitor() monitoring.Monitor {
	return monitor
}

func InitMonitor() {
	conf := Conf()
	if !conf.Global.MonitorEnable {
		log.Info("unable monitor")
		return
	}
	log.Info("init monitor...")
	monitor = monitoring.Load(conf.Global.Monitor)
	cluster := fmt.Sprintf("%s_%d", conf.Global.Name, conf.Global.ClusterID)
	monitor.Init(cluster, conf.Masters.Self().Address, conf.Global.PushGateway, time.Duration(conf.Global.PushInterval)*time.Second)

	monitor.RegisterGauge(cluster, "app", "start_time", []string{"role"})
	monitor.RegisterGauge(cluster, "app", "master", nil)
	monitor.RegisterGauge(cluster, "process", "runtime", []string{"role","type"})
	monitor.RegisterGauge(cluster, "process", "load", []string{"role","type"})
	monitor.RegisterGauge(cluster, "process", "disk", []string{"role","path","type"})
	monitor.RegisterGauge(cluster, "process", "mem", []string{"role","type"})
	monitor.RegisterGauge(cluster, "process", "net", []string{"role","type"})
	monitor.RegisterGauge(cluster, "process", "cpu", []string{"role","type"})

	monitor.RegisterGauge(cluster, "rpc", "cluster", []string{"role","type", "status"})
	monitor.RegisterGauge(cluster, "rpc", "http", []string{"role","status"})

	monitor.RegisterGauge(cluster, "database", "count", nil)
	monitor.RegisterGauge(cluster, "database", "table_count", []string{"name"})
	monitor.RegisterGauge(cluster, "table", "size", []string{"db_name", "table_name"})
	monitor.RegisterGauge(cluster, "table", "range_count", []string{"db_name", "table_name"})
	monitor.RegisterGauge(cluster, "table", "bytes_write", []string{"db_name", "table_name"})
	monitor.RegisterGauge(cluster, "table", "bytes_read", []string{"db_name", "table_name"})
	monitor.RegisterGauge(cluster, "table", "keys_write", []string{"db_name", "table_name"})
	monitor.RegisterGauge(cluster, "table", "keys_read", []string{"db_name", "table_name"})
	monitor.RegisterGauge(cluster, "node", "count", nil)
	monitor.RegisterGauge(cluster, "node", "range_leader_count", []string{"id", "address"})
	monitor.RegisterGauge(cluster, "node", "range_count", []string{"id", "address"})
	monitor.RegisterGauge(cluster, "node", "bytes_write", []string{"id", "address"})
	monitor.RegisterGauge(cluster, "node", "bytes_read", []string{"id", "address"})
	monitor.RegisterGauge(cluster, "node", "capacity", []string{"id", "address"})
	monitor.RegisterGauge(cluster, "node", "used", []string{"id", "address"})
	monitor.RegisterGauge(cluster, "node", "status", []string{"id", "address", "status"})
	monitor.RegisterGauge(cluster, "range", "count", nil)
	monitor.RegisterGauge(cluster, "range", "bytes_write", []string{"db_name", "table_name", "id"})
	monitor.RegisterGauge(cluster, "range", "bytes_read", []string{"db_name", "table_name", "id"})
	monitor.RegisterGauge(cluster, "range", "keys_write", []string{"db_name", "table_name", "id"})
	monitor.RegisterGauge(cluster, "range", "keys_read", []string{"db_name", "table_name", "id"})
	monitor.RegisterGauge(cluster, "event", "range_split", nil)
	monitor.RegisterGauge(cluster, "event", "range_leader_change", nil)
	monitor.RegisterGauge(cluster, "event", "range_snapshot", nil)
	monitor.RegisterGauge(cluster, "event", "range_peer_change", nil)
	monitor.RegisterGauge(cluster, "schedule", "count", nil)
	monitor.RegisterGauge(cluster, "schedule", "event", []string{"type", "status"})
	log.Info("monitor start...")
	monitor.Start()
}

