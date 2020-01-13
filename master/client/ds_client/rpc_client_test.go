// Copyright 2019 The JIMDB Authors
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

package client

import (
	"context"
	"fmt"
	"github.com/jimdb-org/jimdb/master/utils/log"
	"testing"
)

func TestRpcClient(t *testing.T) {
	client := NewDSRpcClient("192.168.212.64:8817", func(addr string) (*ConnTimeout, error) {
		log.Warn("conn to %s dialTimeout:%d writeTimeout:%d,readTimeout:%d", addr, dialTimeout, WriteTimeout, ReadTimeout)
		conn, err := DialTimeout(addr, dialTimeout)
		if err != nil {
			log.Error("conn to %s err :%s", addr, err.Error())
			return nil, err
		}
		conn.SetWriteTimeout(WriteTimeout)
		conn.SetReadTimeout(ReadTimeout)
		return conn, nil
	})

	header, response, err := client.IsAlive(context.Background())

	fmt.Println(header, response, err)
}
