package baudtime

import (
	"sync/atomic"
	"time"
	"github.com/baudtime/baudtime/msg"
	ts "github.com/baudtime/baudtime/util/time"
	"github.com/rcrowley/go-metrics"
	"github.com/prometheus/client_golang/prometheus"
)

type Label struct {
	Name    string
	Value   string
}

type Meta struct {
	NameSpace  string
	SubSystem  string
	Name       string
	InnerLabels []Label
	Labels     []Label
}

func NewMeta(nameSpace, subSystem, name string) *Meta {
	m := &Meta{NameSpace: nameSpace, SubSystem: subSystem, Name: name}
	m.AddLabel(Instance)
	return m
}

func NewMetaWithLabels(nameSpace, subSystem, name string, labels []string) *Meta {
	m := &Meta{NameSpace: nameSpace, SubSystem: subSystem, Name: name}
	m.AddLabel(Instance)
	for _, l := range labels {
		m.AddLabel(l)
	}
	return m
}

func (m *Meta) AddLabel(name string) {
	if name == Instance {
		m.InnerLabels = append(m.InnerLabels, Label{Name: name})
		return
	}
	m.Labels = append(m.Labels, Label{Name: name})
}

func (m *Meta) SetLabel(l Label) {
	for i := 0; i < len(m.InnerLabels); i++ {
		if m.InnerLabels[i].Name == l.Name {
			m.InnerLabels[i].Value = l.Value
			return
		}
	}
	for i := 0; i < len(m.Labels); i++ {
		if m.Labels[i].Name == l.Name {
			m.Labels[i].Value = l.Value
			break
		}
	}
}

// deep clone
func (m *Meta) Clone() Meta {
	c := Meta{
		NameSpace: m.NameSpace,
		SubSystem: m.SubSystem,
		Name: m.Name,
	}
	c.InnerLabels = make([]Label, 0, len(m.InnerLabels))
	for _, l := range m.InnerLabels {
		c.InnerLabels = append(c.InnerLabels, l)
	}
	c.Labels = make([]Label, 0, len(m.Labels))
	for _, l := range m.Labels {
		c.Labels = append(c.Labels, l)
	}
	return c
}

func (m *Meta) MetricName() string {
	return prometheus.BuildFQName(m.NameSpace, m.SubSystem, m.Name)
}

func (m *Meta) MetricLabels() []msg.Label {
	var lbs []msg.Label
	lbs = append(lbs, msg.Label{"__name__", m.MetricName()})
	for _, lb := range m.InnerLabels {
		lbs = append(lbs, msg.Label{Name: lb.Name, Value: lb.Value})
	}
	for _, lb := range m.Labels {
		lbs = append(lbs, msg.Label{Name: lb.Name, Value: lb.Value})
	}
	return lbs
}

type Counter struct {
	Meta
	metrics.Counter
}

type CounterVec struct {
	Collector
}

func NewCounter(metadata Meta) *Counter {
	return &Counter{metadata, metrics.NewCounter()}
}

func NewCounterVec(metadata Meta) *CounterVec {
	return &CounterVec{&MetricMap{
		Meta: metadata,
		newMetric: func(m Meta, lvs []string) Metric {
			// TODO check valid lvs
			if len(m.Labels) != len(lvs) {
				panic("invalid label value")
			}
			for i, lv := range lvs {
				m.Labels[i].Value = lv
			}
			return NewCounter(m)
		},
	},
	}
}

func(cv *CounterVec)WithLabelValues(lvs ...string) *Counter {
	if m, ok := cv.CreateMetricIfMissing(lvs); ok {
		return m.(*Counter)
	} else {
		panic("invalid label values")
	}
}

func (cv *CounterVec) Name() string {
	return cv.Collector.(Metric).Name()
}

func (cv *CounterVec) ToMetric() []*msg.Series {
	return cv.Collector.(Metric).ToMetric()
}

func(c *Counter) Inc() {
	c.Counter.Inc(1)
}

func (c *Counter) ToMetric() []*msg.Series {
	lbs := c.MetricLabels()
	t := ts.FromTime(time.Now())
	points := []msg.Point{{t, float64(c.Count())}}
	return []*msg.Series{&msg.Series{
		Labels: lbs,
		Points: points,
	}}
}

func (c *Counter) MetaC() Meta {
	return c.Meta
}

func (c *Counter) Name() string {
	return c.MetricName()
}

type Gauge struct {
	Meta
	value *int64
}

type GaugeVec struct {
	Collector
}

func NewGauge(m Meta) *Gauge {
	return &Gauge{Meta: m, value: new(int64)}
}

func NewGaugeVec(metadata Meta) *GaugeVec {
	return &GaugeVec{&MetricMap{
		Meta: metadata,
		newMetric: func(m Meta, lvs []string) Metric {
			// TODO check valid lvs
			if len(m.Labels) != len(lvs) {
				panic("invalid label value")
			}
			for i, lv := range lvs {
				m.Labels[i].Value = lv
			}
			return NewGauge(m)
		},
	},
	}
}

func(gv *GaugeVec)WithLabelValues(lvs ...string) *Gauge {
	if m, ok := gv.CreateMetricIfMissing(lvs); ok {
		return m.(*Gauge)
	} else {
		panic("invalid label values")
	}
}

func (gv *GaugeVec) Name() string {
	return gv.Collector.(Metric).Name()
}

func (gv *GaugeVec) ToMetric() []*msg.Series {
	return gv.Collector.(Metric).ToMetric()
}

func (g *Gauge) Set(f float64) {
	atomic.StoreInt64(g.value, int64(f))
}

func (g *Gauge)Inc() {
	atomic.AddInt64(g.value, 1)
}

func (g *Gauge)Dec() {
	atomic.AddInt64(g.value, -1)
}

func (g *Gauge)Add(f float64) {
	atomic.AddInt64(g.value, int64(f))
}

func (g *Gauge)Sub(f float64) {
	atomic.AddInt64(g.value, int64(f)*-1)
}

func (g *Gauge) ToMetric() []*msg.Series {
	lbs := g.MetricLabels()
	t := ts.FromTime(time.Now())
	points := []msg.Point{{t, float64(atomic.LoadInt64(g.value))}}
	return []*msg.Series{&msg.Series{
		Labels: lbs,
		Points: points,
	}}
}

func (g *Gauge) MetaC() Meta {
	return g.Meta
}

func (g *Gauge) Name() string {
	return g.MetricName()
}

//type Histogram struct {
//	Meta
//	maxVal int64
//	mu     struct {
//		syncutil.Mutex
//		cumulative *hdrhistogram.Histogram
//	}
//}
//
//type HistogramVec struct {
//	MetricMap
//}
//
//func NewHistogram(metadata Meta, maxVal int64, sigFigs int) *Histogram {
//	h := &Histogram{
//		Meta: metadata,
//		maxVal:   maxVal,
//	}
//	h.mu.cumulative = hdrhistogram.New(0, maxVal, sigFigs)
//	return h
//}
//
//func NewHistogramVec(metadata Meta,maxVal int64, sigFigs int) *HistogramVec {
//	return &HistogramVec{MetricMap{
//		Meta: metadata,
//		newMetric: func(m Meta, lvs []string) Metric {
//			// TODO check valid lvs
//			if len(m.Labels) != len(lvs) {
//				panic("invalid label value")
//			}
//			for i, lv := range lvs {
//				m.Labels[i].Value = lv
//			}
//			return NewHistogram(m, maxVal, sigFigs)
//		},
//	},
//	}
//}
//
//func(hv *HistogramVec)WithLabelValues(lvs ...string) *Histogram {
//	if m, ok := hv.CreateMetricIfMissing(lvs); ok {
//		return m.(*Histogram)
//	} else {
//		panic("invalid label values")
//	}
//}
//
//func (h *Histogram) Observe(v float64) {
//	h.mu.Lock()
//	defer h.mu.Unlock()
//
//	if h.mu.cumulative.RecordValue(int64(v)) != nil {
//		_ = h.mu.cumulative.RecordValue(h.maxVal)
//	}
//}
//
//// Min returns the minimum.
//func (h *Histogram) Min() int64 {
//	h.mu.Lock()
//	defer h.mu.Unlock()
//	return h.mu.cumulative.Min()
//}
//
//// ToPrometheusMetric returns a filled-in prometheus metric of the right type.
//func (h *Histogram) ToMetric() []*msg.Series {
//	h.mu.Lock()
//	bars := h.mu.cumulative.Distribution()
//	ss := make([]*msg.Series, 0, len(bars)+2)
//	t := ts.FromTime(time.Now())
//	var cumCount uint64
//	var sum float64
//	for _, bar := range bars {
//		if bar.Count == 0 {
//			// No need to expose trivial buckets.
//			continue
//		}
//		upperBound := float64(bar.To)
//		sum += upperBound * float64(bar.Count)
//
//		cumCount += uint64(bar.Count)
//		curCumCount := cumCount // need a new alloc thanks to bad proto code
//		lbs := h.MetricLabels()
//		// modify _name_
//		lbs[0].Value = lbs[0].Value + "_bucket"
//		lbs = append(lbs, msg.Label{Name: "le", Value: fmt.Sprintf("%d", upperBound)})
//		points := []msg.Point{{t, float64(curCumCount)}}
//		ss = append(ss, &msg.Series{
//			Labels: lbs,
//			Points: points,
//		})
//	}
//	lbs := h.MetricLabels()
//	lbs[0].Value = lbs[0].Value + "_count"
//	points := []msg.Point{{t, float64(cumCount)}}
//	ss = append(ss, &msg.Series{
//		Labels: lbs,
//		Points: points,
//	})
//	lbs = h.MetricLabels()
//	lbs[0].Value = lbs[0].Value + "_sum"
//	points = []msg.Point{{t, float64(sum)}}
//	ss = append(ss, &msg.Series{
//		Labels: lbs,
//		Points: points,
//	})
//	h.mu.Unlock()
//
//	return ss
//}
//
//func (h *Histogram) MetaC() Meta {
//	return h.Meta
//}
//
//func (h *Histogram) Name() string {
//	return h.MetricName()
//}

type Collector interface {
	CreateMetricIfMissing(lvs []string) (Metric, bool)
}
