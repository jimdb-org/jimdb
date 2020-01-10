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
package monitoring

import (
	"context"
	"runtime"
	"runtime/debug"
	"time"

	"github.com/jimdb-org/jimdb/jimkv/master/utils/log"
	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/disk"
	"github.com/shirou/gopsutil/load"
	"github.com/shirou/gopsutil/mem"
	"github.com/shirou/gopsutil/net"
)

var lastIoStat net.IOCountersStat
var lastIoTime time.Time
var lastDiskIoStatM map[string]disk.IOCountersStat
var lastDiskIoTime time.Time
var lastGCStats *debug.GCStats
var lastGCTime time.Time

func ProcessMonitor(ctx context.Context, m Monitor, interval time.Duration) {
	if m == nil {
		return
	}
	log.Info("start process monitor ...")
	defer func() {
		log.Info("stop process monitor")
	}()
	// TODO start time
	m.GetGauge(m.GetCluster(), "app", "start_time", "master").Set(float64(time.Now().Second()))
	// TODO
	m.GetGauge(m.GetCluster(), "app", "master").Set(1)
	timer := time.NewTimer(interval)
	for {
		select {
		case <-ctx.Done():
			m.Stop()
			return
			// TODO monitor interval
		case <-timer.C:
			processCPU(m)
			processLoad(m)
			processMEM(m)
			processNetwork(m)
			processDisk(m)
			processRuntime(m)
		    timer.Reset(interval)
		}
	}
}

func processCPU(m Monitor) {
	defer func() {
		if e := recover(); e != nil {
			printStack()
		}
	}()
	ts, err := cpu.Times(false)
	if err != nil {
		return
	}
	m.GetGauge(m.GetCluster(), "process", "cpu", "master", "used").Set(float64(ts[0].User))
}

func processMEM(m Monitor) {
	defer func() {
		if e := recover(); e != nil {
			printStack()
		}
	}()
	mm, err := mem.VirtualMemory()
	if err != nil {
		return
	}
	m.GetGauge(m.GetCluster(), "process", "mem", "master", "total").Set(float64(mm.Total))
	m.GetGauge(m.GetCluster(), "process", "mem", "master", "free").Set(float64(mm.Free))
	m.GetGauge(m.GetCluster(), "process", "mem", "master", "used").Set(float64(mm.Used))
}

func processNetwork(m Monitor) {
	defer func() {
		if e := recover(); e != nil {
			printStack()
		}
	}()
	io, err := net.IOCounters(false)
	if err != nil {
		return
	}
	if lastIoStat == (net.IOCountersStat{}) {
		lastIoStat = io[0]
		lastIoTime = time.Now()
	} else {
		s := io[0]
		m.GetGauge(m.GetCluster(), "process", "net", "master", "bytes_recv").Set(float64(s.BytesRecv-lastIoStat.BytesRecv) / time.Since(lastIoTime).Seconds())
		m.GetGauge(m.GetCluster(), "process", "net", "master", "bytes_send").Set(float64(s.BytesSent-lastIoStat.BytesSent) / time.Since(lastIoTime).Seconds())
		m.GetGauge(m.GetCluster(), "process", "net", "master", "packets_recv").Set(float64(s.PacketsRecv-lastIoStat.PacketsRecv) / time.Since(lastIoTime).Seconds())
		m.GetGauge(m.GetCluster(), "process", "net", "master", "packets_send").Set(float64(s.PacketsSent-lastIoStat.PacketsSent) / time.Since(lastIoTime).Seconds())
		lastIoStat = s
		lastIoTime = time.Now()
	}
}

func processDisk(m Monitor) {
	defer func() {
		if e := recover(); e != nil {
			printStack()
		}
	}()
	pss, err := disk.Partitions(true)
	if err != nil {
		return
	}
	var names []string
	for _, ps := range pss {
		names = append(names, ps.Device)
	}
	ioss, err := disk.IOCounters(names...)
	if err != nil {
		return
	}
	if lastDiskIoStatM == nil {
		lastDiskIoStatM = make(map[string]disk.IOCountersStat)
		for name, ios := range ioss {
			copyIos := ios
			lastDiskIoStatM[name] = copyIos
		}
		lastDiskIoTime = time.Now()
	} else {
		for name, ios := range ioss {
			if s, ok := lastDiskIoStatM[name]; ok {
				m.GetGauge(m.GetCluster(), "process", "disk", "master",name, "read_bytes").Set(float64(ios.ReadBytes-s.ReadBytes) / time.Since(lastDiskIoTime).Seconds())
				m.GetGauge(m.GetCluster(), "process", "disk", "master",name, "write_bytes").Set(float64(ios.WriteBytes-s.WriteBytes) / time.Since(lastDiskIoTime).Seconds())
				m.GetGauge(m.GetCluster(), "process", "disk", "master",name, "packets_recv").Set(float64(ios.ReadCount-s.ReadCount) / time.Since(lastDiskIoTime).Seconds())
				m.GetGauge(m.GetCluster(), "process", "disk", "master",name, "packets_send").Set(float64(ios.WriteCount-s.WriteCount) / time.Since(lastDiskIoTime).Seconds())
			}
			copyIos := ios
			// update
			lastDiskIoStatM[name] = copyIos
		}
		lastDiskIoTime = time.Now()
	}
}

func processLoad(m Monitor) {
	defer func() {
		if e := recover(); e != nil {
			printStack()
		}
	}()
	l, err := load.Avg()
	if err != nil {
		return
	}
	m.GetGauge(m.GetCluster(), "process", "load", "master","load1").Set(float64(l.Load1))
	m.GetGauge(m.GetCluster(), "process", "load", "master","load5").Set(float64(l.Load5))
	m.GetGauge(m.GetCluster(), "process", "load", "master","load15").Set(float64(l.Load15))
}

func processRuntime(m Monitor) {
	defer func() {
		if e := recover(); e != nil {
			printStack()
		}
	}()
	gc := &debug.GCStats{}
	debug.ReadGCStats(gc)

	numGoroutine := runtime.NumGoroutine()
	m.GetGauge(m.GetCluster(), "process", "runtime", "master", "goroutine").Set(float64(numGoroutine))

	if lastGCStats == nil {
		lastGCStats = gc
		lastGCTime = time.Now()
	} else {
		gcPausePercent := float64(gc.PauseTotal-lastGCStats.PauseTotal) / float64(time.Since(lastGCTime))
		m.GetGauge(m.GetCluster(), "process", "runtime", "master","gc_pause_percent").Set(float64(gcPausePercent))
	}
	m.GetGauge(m.GetCluster(), "process", "runtime", "master","gc_num").Set(float64(gc.NumGC))
	m.GetGauge(m.GetCluster(), "process", "runtime", "master","gc_pause").Set(float64(gc.PauseTotal))
	lastGCStats = gc
	lastGCTime = time.Now()
}

func printStack() {
	var buf [4096]byte
	n := runtime.Stack(buf[:], false)
	log.Info("==> %s\n", string(buf[:n]))
}
