/*
 * Copyright 2019 The Baudtime Authors
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package util

import (
	"fmt"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
)

type Jaegerlogger struct {
	Logger log.Logger
}

func (l *Jaegerlogger) Error(err string) {
	level.Error(l.Logger).Log("err", err)
}

func (l *Jaegerlogger) Infof(msg string, args ...interface{}) {
	level.Info(l.Logger).Log("msg", fmt.Sprintf(msg, args...))
}
