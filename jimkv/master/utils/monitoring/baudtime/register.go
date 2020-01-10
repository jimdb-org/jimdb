package baudtime

import (
	"sync"
	"fmt"
	"github.com/baudtime/baudtime/msg"
)

var register  *registerI

type Metric interface {
	Name()  string
	ToMetric() []*msg.Series
}

type MetricMap struct {
	Meta
	ms      sync.Map
	newMetric func(m Meta, lvs []string) Metric
}

func (mm *MetricMap) Name()  string {
	return mm.MetricName()
}

func (mm *MetricMap) ToMetric() []*msg.Series {
	var ms []*msg.Series
	mm.ms.Range(func(key, value interface{}) bool {
		ms = append(ms, value.(Metric).ToMetric()...)
		return true
	})
	return ms
}

func (mm *MetricMap) ValidLabels(lvs []string) bool {
	return len(mm.Meta.Labels) == len(lvs)
}

func (mm *MetricMap) CreateMetricIfMissing(lvs []string) (Metric, bool) {
	if !mm.ValidLabels(lvs) {
		return nil, false
	}
	// TODO check invalid label

	h := hashNew()
	for _, lv := range lvs {
		h = hashAdd(h, lv)
		h = hashAddByte(h, separatorByte)
	}
	e, ok := mm.ms.Load(h)
	if ok {
		return e.(Metric), true
	}
	e, _ = mm.ms.LoadOrStore(h, mm.newMetric(mm.Meta.Clone(), lvs))
	return e.(Metric), true
}

type registerI struct {
	metrics   sync.Map
}

func (r *registerI) Register(m Metric) {
	_, load := r.metrics.LoadOrStore(m.Name(), m)
	if load {
		panic(fmt.Sprintf("Duplicate register [%s]", m.Name()))
	}
}

func (r *registerI) GetMetric(name string) (Metric, bool) {
	if e, ok := r.metrics.Load(name); ok {
		return e.(Metric), ok
	} else {
		return nil, false
	}
}

func (r *registerI) GetMetricWithLvs(name string, lvs []string) (Metric, bool) {
	m, ok := r.GetMetric(name)
	if !ok {
		return nil, false
	}
	return m.(Collector).CreateMetricIfMissing(lvs)
}

func (r *registerI) Range(f func(string,  Metric) bool) {
	r.metrics.Range(func(key, value interface{}) bool {
		name := key.(string)
		m := value.(Metric)
		return f(name, m)
	})
}

func Register(m Metric) {
	register.Register(m)
}

func GetMetric(name string) (Metric, bool) {
	return register.GetMetric(name)
}

func GetMetricWithLvs(name string, lvs []string) (Metric, bool) {
	return register.GetMetricWithLvs(name, lvs)
}

func Range(f func(string,  Metric) bool) {
	register.Range(f)
}

func init() {
	register = &registerI{}
}


