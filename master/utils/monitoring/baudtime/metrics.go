package baudtime

import (
	"context"
	"net"
	"strconv"
	"time"
	"sync"
	"fmt"
	"github.com/jimdb-org/jimdb/master/utils/log"
	"github.com/baudtime/baudtime/tcp/client"
	"github.com/baudtime/baudtime/msg/gateway"
	"github.com/jimdb-org/jimdb/master/utils/monitoring"
)

const Name = "baudtime"
const Instance = "instance"
var _ monitoring.Monitor = &Monitor{}

type MKey struct {
	Namespace   string
	Subsystem   string
	Name        string
}

type Monitor struct {
	instance    string
	counters    sync.Map
	gauges      sync.Map
	observers   sync.Map
	lock        sync.Mutex
	readOnly    bool
	closed      bool
	cluster     string
	pushGateway string
	pushInterval time.Duration
	pusher      *Pusher
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
	m.pusher = NewPusher(pushGateway)
	m.done = make(chan struct{})
	return
}

// register must call before Start()
func(m *Monitor) RegisterCounter(nameSpace, subSystem, name string, labelNames []string) {
	if !m.readOnly {
		if nameSpace != m.cluster {
			panic(fmt.Sprintf("invaid name space %s", nameSpace))
		}
		if len(labelNames) > 0 {
			meta := NewMetaWithLabels(nameSpace, subSystem, name, labelNames)
			meta.SetLabel(Label{Name: Instance, Value: m.instance})
			counter := NewCounterVec(*meta)
			if _, ok := m.counters.LoadOrStore(MKey{nameSpace, subSystem, name}, counter); ok {
				panic(fmt.Sprintf("repeated registration counter for %s %s %s", nameSpace, subSystem, name))
			} else {
				Register(counter)
			}
		} else {
			meta := NewMeta(nameSpace, subSystem, name)
			meta.SetLabel(Label{Name: Instance, Value: m.instance})
			counter := NewCounter(*meta)
			if _, ok := m.counters.LoadOrStore(MKey{nameSpace, subSystem, name}, counter); ok {
				panic(fmt.Sprintf("repeated registration counter for %s %s %s", nameSpace, subSystem, name))
			} else {
				Register(counter)
			}
		}

	} else {
		panic("monitor already start")
	}
}

func(m *Monitor) RegisterGauge(nameSpace, subSystem, name string, labelNames []string) {
	if !m.readOnly {
		if nameSpace != m.cluster {
			panic(fmt.Sprintf("invaid name space %s", nameSpace))
		}
		if len(labelNames) > 0 {
			meta := NewMetaWithLabels(nameSpace, subSystem, name, labelNames)
			meta.SetLabel(Label{Name: Instance, Value: m.instance})
			gauge := NewGaugeVec(*meta)
			if _, ok := m.gauges.LoadOrStore(MKey{nameSpace, subSystem, name}, gauge); ok {
				panic(fmt.Sprintf("repeated registration gauge for %s %s %s", nameSpace, subSystem, name))
			} else {
				Register(gauge)
			}
		} else {
			meta := NewMeta(nameSpace, subSystem, name)
			meta.SetLabel(Label{Name: Instance, Value: m.instance})
			gauge := NewGauge(*meta)
			if _, ok := m.gauges.LoadOrStore(MKey{nameSpace, subSystem, name}, gauge); ok {
				panic(fmt.Sprintf("repeated registration gauge for %s %s %s", nameSpace, subSystem, name))
			} else {
				Register(gauge)
			}
		}

	} else {
		panic("monitor already start")
	}
}

// after start(), call register will panic
func(m *Monitor) Start() {
	meta := NewMetaWithLabels("", "", "cluster_info", []string{"cluster"})
	meta.SetLabel(Label{Name: Instance, Value: m.instance})
	gauge := NewGaugeVec(*meta)
	if _, ok := m.gauges.LoadOrStore(MKey{"", "", "cluster_info"}, gauge); ok {
		panic(fmt.Sprintf("repeated registration gauge for %s %s %s", "", "", "cluster_info"))
	} else {
		Register(gauge)
	}
	m.readOnly = true
	gauge.WithLabelValues(m.cluster).Set(1)
	go func() {
		log.Info("start metric push...")
		timer := time.NewTimer(m.pushInterval)
		for {
			select {
			case <-m.done:
				return
			case <-timer.C:
				if err := m.pusher.Push(); err != nil {
					log.Warn("baudtime push error %v", err)
				}
				timer.Reset(m.pushInterval)
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

func (m *Monitor) Push() {
	if err := m.pusher.Push(); err != nil {
		log.Warn("baudtime push error %v", err)
	}
}

func(m *Monitor) GetCounter(nameSpace, subSystem, name string, lvs ...string) monitoring.Counter {
	if m.readOnly {
		if val, ok := m.counters.Load(MKey{nameSpace, subSystem, name}); ok {
			if lvs == nil {
				return val.(*Counter)
			} else {
				return val.(*CounterVec).WithLabelValues(lvs...)
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
				return val.(*Gauge)
			} else {
				return val.(*GaugeVec).WithLabelValues(lvs...)
			}
		} else {
			panic(fmt.Sprintf("gauge[%s %s %s] must not register first", nameSpace, subSystem, name))
		}
	} else {
		panic("monitor do not running")
	}
}

type Pusher struct {
	cli     *client.Client
}

func NewPusher(url string) *Pusher {
	host, port, err := net.SplitHostPort(url)
	if err != nil {
		panic(fmt.Sprintf("invalid pusher url %s", url))
	}
	p, err := strconv.Atoi(port)
	if err != nil {
		panic(fmt.Sprintf("invalid pusher url %s", url))
	}
	addrProvider := client.NewDnsAddrProvider(host, p)
	cli := client.NewGatewayClient("metric", addrProvider)
	return &Pusher{cli: cli}
}

func (p *Pusher) Push() error {
	r := &gateway.AddRequest{}
	Range(func(name string, m Metric) bool {
		r.Series = append(r.Series, m.ToMetric()...)
		return true
	})
	_, err := p.cli.SyncRequest(context.Background(), r)
	return err
}

func init() {
	monitoring.Register(Name, New())
}
