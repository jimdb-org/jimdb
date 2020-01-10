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

package client

import (
	"go.uber.org/multierr"
	"sync"
	"sync/atomic"
	"unsafe"
)

type ConnPool interface {
	GetConn() (*Conn, error)
	Destroy(c *Conn) error
	Close() error
}

type ServiceConnPool struct {
	conns     *sync.Map
	addrProv  ServiceAddrProvider
	new       func(address string) (*Conn, error)
	onConnect func(*Conn)
	onClose   func(*Conn)
}

func (pool *ServiceConnPool) GetConn() (*Conn, error) {
	address, err := pool.addrProv.GetServiceAddr()
	if err != nil {
		return nil, err
	}

	c, found := pool.conns.Load(address)
	if !found {
		newc, err := pool.new(address)
		if err != nil {
			pool.addrProv.ServiceDown(address)
			return nil, err
		}

		var loaded bool
		c, loaded = pool.conns.LoadOrStore(address, newc)

		if loaded {
			newc.close()
		} else {
			if pool.onConnect != nil {
				pool.onConnect(newc)
			}
		}
	}
	return c.(*Conn), nil
}

func (pool *ServiceConnPool) Destroy(c *Conn) error {
	pool.conns.Delete(c.address)
	if pool.onClose != nil {
		pool.onClose(c)
	}
	return c.close()
}

func (pool *ServiceConnPool) Close() error {
	var multiErr error
	pool.conns.Range(func(key, value interface{}) bool {
		err := value.(*Conn).close()
		if err != nil {
			multiErr = multierr.Append(multiErr, err)
		}
		return true
	})
	pool.conns = nil
	pool.addrProv.StopWatch()
	return multiErr
}

type HostConnPool struct {
	//do not use sync.Pool
	iter    uint64
	size    int
	conns   []*Conn
	address string

	new       func(address string) (*Conn, error)
	onConnect func(*Conn)
	onClose   func(*Conn)
}

func (pool *HostConnPool) GetConn() (*Conn, error) {
	iter := atomic.AddUint64(&pool.iter, 1) % uint64(pool.size)
	c := (*Conn)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&pool.conns[iter]))))

	if c != nil && !c.isClosed() {
		return c, nil
	}

	newc, err := pool.new(pool.address)
	if err != nil {
		return nil, err
	}
	if pool.onConnect != nil {
		pool.onConnect(newc)
	}

	if atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&pool.conns[iter])), unsafe.Pointer(c), unsafe.Pointer(newc)) {
		c = newc
	} else {
		newc.close()
		c = (*Conn)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&pool.conns[iter]))))
	}

	return c, nil
}

func (pool *HostConnPool) Destroy(c *Conn) (err error) {
	if pool.onClose != nil {
		pool.onClose(c)
	}
	return c.close()
}

func (pool *HostConnPool) Close() error {
	var multiErr error
	for _, c := range pool.conns {
		if c != nil && !c.isClosed() {
			if err := c.close(); err != nil {
				multiErr = multierr.Append(multiErr, err)
			}
		}
	}
	return multiErr
}
