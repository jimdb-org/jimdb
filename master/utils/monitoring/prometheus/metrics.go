// Copyright 2018 The JIMDB Authors.
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
package prometheus

import (
	"sync"
	"fmt"
	"time"

	"github.com/jimdb-org/jimdb/master/utils/monitoring"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"

	"github.com/jimdb-org/jimdb/master/utils/log"
)

const Name = "prometheus"
var _ monitoring.Monitor = &Monitor{}
const pushJob = "push_gateway"

type MKey struct {
	Namespace   string
	Subsystem   string
	Name        string
}

type Monitor struct {
	counters    sync.Map
	gauges      sync.Map
	observers   sync.Map
	lock        sync.Mutex
	readOnly    bool
	closed      bool
	instance    string
	cluster     string
	pushGateway string
	pushInterval time.Duration
	pusher      *push.Pusher
	done        chan struct{}
}

func New() monitoring.Monitor {
	return &Monitor{}
}

func (m *Monitor) GetInstance() string {
	return m.instance
}

func (m *Monitor) GetCluster() string {
	return m.cluster
}

func (m *Monitor) GetPushGatewayUrl() string {
	return m.pushGateway
}

func(m *Monitor) Init(cluster, instance string, pushGateway string, pushInterval time.Duration) {
	if m.readOnly {
		panic("monitor already start")
	}
	m.cluster = cluster
	m.instance = instance
	m.pushGateway = pushGateway
	m.pushInterval = pushInterval
	m.pusher = push.New(pushGateway, pushJob)
	m.done = make(chan struct{})
	return
}

// register must call before Start()
func(m *Monitor) RegisterCounter(nameSpace, subSystem, name string, labelNames []string) {
	if !m.readOnly {
		if m.instance != nameSpace {
			panic(fmt.Sprintf("invaid name space %s", nameSpace))
		}
		if len(labelNames) > 0 {
			counter := prometheus.NewCounterVec(
				prometheus.CounterOpts{
					Namespace: nameSpace,
					Subsystem: subSystem,
					Name:      name,
				}, labelNames)
			if _, ok := m.counters.LoadOrStore(MKey{nameSpace, subSystem, name}, counter); ok {
				panic(fmt.Sprintf("repeated registration counter for %s %s %s", nameSpace, subSystem, name))
			} else {
				m.pusher.Collector(counter)
			}
		} else {
			counter := prometheus.NewCounter(
				prometheus.CounterOpts{
					Namespace: nameSpace,
					Subsystem: subSystem,
					Name:      name,
				})
			if _, ok := m.counters.LoadOrStore(MKey{nameSpace, subSystem, name}, counter); ok {
				panic(fmt.Sprintf("repeated registration counter for %s %s %s", nameSpace, subSystem, name))
			} else {
				m.pusher.Collector(counter)
			}
		}
	} else {
		panic("monitor already start")
	}
}

func(m *Monitor) RegisterGauge(nameSpace, subSystem, name string, labelNames []string) {
	if !m.readOnly {
		if m.instance != nameSpace {
			panic(fmt.Sprintf("invaid name space %s", nameSpace))
		}
		if len(labelNames) > 0 {
			gauge := prometheus.NewGaugeVec(
				prometheus.GaugeOpts{
					Namespace: nameSpace,
					Subsystem: subSystem,
					Name:      name,
				}, labelNames)
			if _, ok := m.gauges.LoadOrStore(MKey{nameSpace, subSystem, name}, gauge); ok {
				panic(fmt.Sprintf("repeated registration gauge for %s %s %s", nameSpace, subSystem, name))
			} else {
				m.pusher.Collector(gauge)
			}
		} else {
			gauge := prometheus.NewGauge(
				prometheus.GaugeOpts{
					Namespace: nameSpace,
					Subsystem: subSystem,
					Name:      name,
				})
			if _, ok := m.gauges.LoadOrStore(MKey{nameSpace, subSystem, name}, gauge); ok {
				panic(fmt.Sprintf("repeated registration gauge for %s %s %s", nameSpace, subSystem, name))
			} else {
				m.pusher.Collector(gauge)
			}
		}

	} else {
		panic("monitor already start")
	}
}

//func(m *Monitor) RegisterObserver(nameSpace, subSystem, name string, buckets []float64, labelNames []string) {
//	if !m.readOnly {
//		if len(labelNames) > 0 {
//			observer := prometheus.NewHistogramVec(
//				prometheus.HistogramOpts{
//					Namespace: nameSpace,
//					Subsystem: subSystem,
//					Name:      name,
//					Buckets:   buckets,
//				}, labelNames)
//			if _, ok := m.observers.LoadOrStore(MKey{nameSpace, subSystem, name}, observer); ok {
//				panic(fmt.Sprintf("repeated registration gauge for %s %s %s", nameSpace, subSystem, name))
//			} else {
//				m.pusher.Collector(observer)
//			}
//		} else {
//			observer := prometheus.NewHistogram(
//				prometheus.HistogramOpts{
//					Namespace: nameSpace,
//					Subsystem: subSystem,
//					Name:      name,
//					Buckets:   buckets,
//				})
//			if _, ok := m.observers.LoadOrStore(MKey{nameSpace, subSystem, name}, observer); ok {
//				panic(fmt.Sprintf("repeated registration gauge for %s %s %s", nameSpace, subSystem, name))
//			} else {
//				m.pusher.Collector(observer)
//			}
//		}
//	} else {
//		panic("monitor already start")
//	}
//}

// after start(), call register will panic
func(m *Monitor) Start() {
	gauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "",
			Subsystem: "",
			Name:      "cluster_info",
		}, []string{"cluster"})
	m.pusher.Collector(gauge)
	m.readOnly = true
	gauge.WithLabelValues(m.cluster).Set(1)
	go func() {
		for {
			select {
			case <-m.done:
				return
			case <-time.After(m.pushInterval):
				if err := m.pusher.Push(); err != nil {
					log.Warn("prometheus push error %v", err)
				}
			}
		}
	}()
}

func(m *Monitor) Stop() {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.closed {
		return
	}
	m.closed = true
	close(m.done)
}

func(m *Monitor) Push() {
	m.pusher.Push()
}

func(m *Monitor) GetCounter(nameSpace, subSystem, name string, lvs ...string) monitoring.Counter {
	if m.readOnly {
		if val, ok := m.counters.Load(MKey{nameSpace, subSystem, name}); ok {
			if lvs == nil {
				return val.(prometheus.Counter)
			} else {
				return val.(*prometheus.CounterVec).WithLabelValues(lvs...)
			}
		} else {
			panic(fmt.Sprintf("counter[%s %s %s] must not register first", nameSpace, subSystem, name))
		}
	} else {
		panic("monitor do not running")
	}
}

func(m *Monitor) GetGauge(nameSpace, subSystem, name string, lvs ...string) monitoring.Gauge {
	if m.readOnly {
		if val, ok := m.gauges.Load(MKey{nameSpace, subSystem, name}); ok {
			if lvs == nil {
				return val.(prometheus.Gauge)
			} else {
				return val.(*prometheus.GaugeVec).WithLabelValues(lvs...)
			}
		} else {
			panic(fmt.Sprintf("gauge[%s %s %s] must not register first", nameSpace, subSystem, name))
		}
	} else {
		panic("monitor do not running")
	}
}

//func(m *Monitor) GetObserver(nameSpace, subSystem, name string, lvs ...string) monitoring.Observer {
//	if m.readOnly {
//		if val, ok := m.observers.Load(MKey{nameSpace, subSystem, name}); ok {
//			if lvs == nil {
//				return val.(prometheus.Histogram)
//			} else {
//				return val.(*prometheus.HistogramVec).WithLabelValues(lvs...)
//			}
//		} else {
//			panic(fmt.Sprintf("observer[%s %s %s] must not register first", nameSpace, subSystem, name))
//		}
//	} else {
//		panic("monitor do not running")
//	}
//}

func(m *Monitor) GetAlarm(nameSpace, subSystem, name string, lvs ...string) monitoring.Alarm {
	if m.readOnly {
		return &EmptyAlarm{}
	} else {
		panic("monitor do not running")
	}
}

func(m *Monitor) Alive() {
	if m.readOnly {

	} else {
		panic("monitor do not running")
	}
	return
}

type EmptyAlarm struct {

}

func (a *EmptyAlarm) Alarm(string) {
	return
}

func init() {
	monitoring.Register(Name, New())
}
