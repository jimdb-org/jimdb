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
package monitoring

import (
	"sync"
	"fmt"
)

var register sync.Map

func Register(name string, monitor Monitor) {
	_, load := register.LoadOrStore(name, monitor)
	if load {
		panic(fmt.Sprintf("repeated registration for %s", name))
	}
}

func Load(name string) Monitor {
	if val, ok := register.Load(name); ok {
		return val.(Monitor)
	}
	panic(fmt.Sprintf("invalid monitor %s", name))
	return nil
}
