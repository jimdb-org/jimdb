// Copyright 2019 The JimDB Authors
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

import "time"

type Gauge interface {
	// Set sets the Gauge to an arbitrary value.
	Set(float64)
	// Inc increments the Gauge by 1. Use Add to increment it by arbitrary
	// values.
	Inc()
	// Dec decrements the Gauge by 1. Use Sub to decrement it by arbitrary
	// values.
	Dec()
	// Add adds the given value to the Gauge. (The value can be negative,
	// resulting in a decrease of the Gauge.)
	Add(float64)
	// Sub subtracts the given value from the Gauge. (The value can be
	// negative, resulting in an increase of the Gauge.)
	Sub(float64)
}

type Counter interface {
	// Inc increments the counter by 1. Use Add to increment it by arbitrary
	// non-negative values.
	Inc()
	// Add adds the given value to the counter. It panics if the value is <
	// 0.
	// Add(float64)
}

type Alarm interface {
	Alarm(string)
}

type Monitor interface {
	// call init() before all other API
	Init(cluster, instance string, pushGateway string, pushInterval time.Duration)
	// register must call before Start()
	RegisterCounter(nameSpace, subSystem, name string, labelNames []string)
	RegisterGauge(nameSpace, subSystem, name string, labelNames []string)
	//RegisterObserver(nameSpace, subSystem, name string, buckets []float64, labelNames []string)

	// after start(), call register will panic
	Start()
	Stop()
	Push()
	GetInstance() string
	GetCluster() string
	GetPushGatewayUrl() string
	GetCounter(nameSpace, subSystem, name string, lvs ...string) Counter
	GetGauge(nameSpace, subSystem, name string, lvs ...string) Gauge
}

func ExponentialBuckets(start, factor float64, count int) []float64 {
	if count < 1 {
		panic("ExponentialBuckets needs a positive count")
	}
	if start <= 0 {
		panic("ExponentialBuckets needs a positive start value")
	}
	if factor <= 1 {
		panic("ExponentialBuckets needs a factor greater than 1")
	}
	buckets := make([]float64, count)
	for i := range buckets {
		buckets[i] = start
		start *= factor
	}
	return buckets
}