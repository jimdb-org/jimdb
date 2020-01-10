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

package cblog

import (
	"github.com/jimdb-org/jimdb/jimkv/master/utils/assert"
	"github.com/jimdb-org/jimdb/jimkv/master/utils/log"
	"testing"
	"time"
)

func TestBaudLog(t *testing.T) {

	log.Regist(NewBaudLog("test", "Test", "debug", false))

	assert.True(t, log.IsDebugEnabled())

	//log.Regist(baudLog)

	log.Info("hello %s", "info")
	log.Debug("hello %s", "debug")
	log.Warn("hello %s", "warn")
	log.Error("hello %s", "error")
	time.Sleep(1 * time.Second)
}
