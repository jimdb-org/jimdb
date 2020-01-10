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
package baudtime

import (
	"testing"
	"reflect"
	"time"
	"fmt"
)

func TestCollect(t *testing.T) {
	m := New()
	m.Init("t1", "mm", "127.0.0.1:8080", time.Second)
	m.RegisterGauge("t1", "t2", "t3", []string{"l1", "l2"})
	m.(*Monitor).readOnly = true
	metric1 := m.GetGauge("t1", "t2", "t3", "lv1", "lv2")
	metric1.Set(1)
	metric2 := m.GetGauge("t1", "t2", "t3", "lv1", "lv3")
	metric2.Set(2)
	for _, msg := range metric1.(Metric).ToMetric() {
		fmt.Println(msg.Labels)
	}
	for _, msg := range metric2.(Metric).ToMetric() {
		fmt.Println(msg.Labels)
	}
	if reflect.DeepEqual(metric1, metric2) {
		t.Fatalf("test fault")
	}
}
