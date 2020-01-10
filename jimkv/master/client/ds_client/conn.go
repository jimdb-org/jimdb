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
	"github.com/jimdb-org/jimdb/jimkv/master/utils/log"
	"net"
	"sync"
	"time"
)

const (
	WriteTimeout = 10 * time.Second

	ReadTimeout = 31 * time.Second

	HeartbeatInterval = 9 * time.Second

	ReadTimeoutShort  = 20 * time.Second  // For requests that read/write several key-values.
	ReadTimeoutMedium = 60 * time.Second  // For requests that may need scan region.
	ReadTimeoutLong   = 150 * time.Second // For requests that may need scan region multiple times.
)

const (
	dialTimeout        = 5 * time.Second
	DefaultIdleTimeout = 1200 * time.Second
	// 128 KB
	DefaultInitialWindowSize int32 = 1024 * 64
	DefaultPoolSize          int   = 1

	// 40 KB
	DefaultWriteSize = 40960
	// 40 KB
	DefaultReadSize = 40960
)

type ConnTimeout struct {
	addr         string
	lock         sync.Mutex
	close        bool
	conn         net.Conn
	rdTimeout    time.Duration
	wrTimeout    time.Duration
	rdTimeoutRaw time.Duration
	wrTimeoutRaw time.Duration
}

func DialTimeout(addr string, timeout time.Duration) (*ConnTimeout, error) {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, err
	}

	conn.(*net.TCPConn).SetNoDelay(true)
	conn.(*net.TCPConn).SetLinger(0)
	conn.(*net.TCPConn).SetKeepAlive(true)
	return &ConnTimeout{conn: conn, addr: addr, close: false}, nil
}

func NewConnTimeout(conn net.Conn) *ConnTimeout {
	if conn == nil {
		return nil
	}

	conn.(*net.TCPConn).SetNoDelay(true)
	conn.(*net.TCPConn).SetLinger(0)
	conn.(*net.TCPConn).SetKeepAlive(true)
	return &ConnTimeout{conn: conn, addr: conn.RemoteAddr().String(), close: false}
}

func (c *ConnTimeout) SetReadTimeout(timeout time.Duration) {
	if c.rdTimeoutRaw != timeout {
		c.rdTimeoutRaw = timeout
		c.rdTimeout = timeout
	}
}

func (c *ConnTimeout) SetWriteTimeout(timeout time.Duration) {
	if c.wrTimeoutRaw != timeout {
		c.wrTimeoutRaw = timeout
		c.wrTimeout = timeout
	}
}

func (c *ConnTimeout) Write(p []byte) (n int, err error) {
	if err = c.conn.SetWriteDeadline(time.Now().Add(c.wrTimeout)); err != nil {
		return
	}

	n, err = c.conn.Write(p)
	return
}

func (c *ConnTimeout) Read(p []byte) (n int, err error) {
	if err = c.conn.SetReadDeadline(time.Now().Add(c.rdTimeout)); err != nil {
		return
	}
	n, err = c.conn.Read(p)
	return
}

func (c *ConnTimeout) RemoteAddr() string {
	return c.addr
}

func (c *ConnTimeout) Close() error {
	if c.close {
		return nil
	}
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.close {
		return nil
	}
	log.Warn("close conn %s", c.RemoteAddr())
	c.conn.Close()
	c.close = true
	return nil
}
