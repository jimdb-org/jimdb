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

package client

import (
	"errors"
	"github.com/jimdb-org/jimdb/jimkv/master/utils/log"
	"sync"
	"time"
)

type createConnFunc func(addr string) (RpcClient, error)

type Pool struct {
	size int64
	pool []RpcClient
}

func NewPool(size int, addr string, fun createConnFunc) (*Pool, error) {
	var pool []RpcClient
	for i := 0; i < size; i++ {
		cli, err := fun(addr)
		if err != nil {
			return nil, err
		}
		pool = append(pool, cli)
	}
	return &Pool{size: int64(size), pool: pool}, nil
}

func (p *Pool) GetConn() RpcClient {
	index := time.Now().UnixNano() % p.size
	return p.pool[index]
}

func (p *Pool) Close() {
	for _, c := range p.pool {
		c.Close()
	}
}

type ResourcePool struct {
	lock sync.RWMutex
	size int
	set  map[string]*Pool
}

func NewResourcePool(size int) *ResourcePool {
	return &ResourcePool{size: size, set: make(map[string]*Pool)}
}

func (rp *ResourcePool) Close() {
	rp.lock.RLock()
	defer rp.lock.RUnlock()
	for _, pool := range rp.set {
		pool.Close()
	}
}

func (rp *ResourcePool) GetConn(addr string) (RpcClient, error) {
	if len(addr) == 0 {
		return nil, errors.New("invalid address")
	}
	var pool *Pool
	var ok bool
	var err error
	rp.lock.RLock()
	if pool, ok = rp.set[addr]; ok {
		rp.lock.RUnlock()
		return pool.GetConn(), nil
	}
	rp.lock.RUnlock()
	rp.lock.Lock()
	defer rp.lock.Unlock()
	if pool, ok = rp.set[addr]; ok {
		return pool.GetConn(), nil
	}

	pool, err = NewPool(rp.size, addr, func(_addr string) (RpcClient, error) {
		cli := NewDSRpcClient(addr, func(addr string) (*ConnTimeout, error) {
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
		return cli, nil
	})
	if err != nil {
		log.Error("new pool[address %s] failed, err[%v]", addr, err)
		return nil, err
	}

	rp.set[addr] = pool
	return pool.GetConn(), nil
}
