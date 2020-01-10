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
	"context"
	"fmt"
	"github.com/baudtime/baudtime/msg"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/baudtime/baudtime/tcp"
	"github.com/pkg/errors"
)

var timeoutErr = errors.New("timeout")

type Callback func(opaque uint64, response msg.Message)

type Future struct {
	opaque    uint64
	timestamp time.Time
	ch        chan msg.Message
	callback  Callback
	err       error
}

func newFuture(opaque uint64, callback Callback) *Future {
	return &Future{
		opaque:    opaque,
		timestamp: time.Now(),
		ch:        make(chan msg.Message, 1),
		callback:  callback,
	}
}

func (f *Future) Get(ctx context.Context) (msg.Message, error) {
	select {
	case msg := <-f.ch:
		return msg, f.err
	case <-ctx.Done():
		return nil, timeoutErr
	}
}

func (f *Future) setErr(err error) {
	f.err = err
}

type futureTable struct {
	sync.RWMutex
	futures map[uint64]*Future
}

func (ftable *futureTable) add(opaque uint64, f *Future) {
	ftable.Lock()
	ftable.futures[opaque] = f
	ftable.Unlock()
}

func (ftable *futureTable) del(opaque uint64) {
	ftable.Lock()
	delete(ftable.futures, opaque)
	ftable.Unlock()
}

func (ftable *futureTable) get(opaque uint64) (*Future, bool) {
	ftable.RLock()
	f, ok := ftable.futures[opaque]
	ftable.RUnlock()
	return f, ok
}

type Conn struct {
	address    string
	nativeConn *net.TCPConn
	rwLoop     *tcp.ReadWriteLoop
	futureTab  *futureTable
}

func newConn(address string) (*Conn, error) {
	c, err := net.DialTimeout("tcp4", address, 2*time.Second)
	if err != nil {
		return nil, err
	}

	tc := c.(*net.TCPConn)
	tc.SetNoDelay(true)
	tc.SetKeepAlive(true)
	tc.SetKeepAlivePeriod(60 * time.Second)
	tc.SetReadBuffer(1024 * 1024)
	tc.SetWriteBuffer(1024 * 1024)

	cc := &Conn{
		address:    address,
		nativeConn: tc,
		futureTab:  &futureTable{futures: make(map[uint64]*Future)},
	}
	cc.rwLoop = tcp.NewReadWriteLoop(tc, func(ctx context.Context, in tcp.Message, b []byte) tcp.Message {
		if f, ok := cc.futureTab.get(in.GetOpaque()); ok {
			f.ch <- in.GetRaw()
			if f.callback != nil {
				f.callback(in.GetOpaque(), in.GetRaw())
			}
			cc.futureTab.del(in.GetOpaque())
		}
		return tcp.EmptyMsg //TODO
	})

	cc.rwLoop.OnExit(func() {
		cc.futureTab.RLock()
		for opaque, f := range cc.futureTab.futures {
			delete(cc.futureTab.futures, opaque)
			close(f.ch)
		}
		cc.futureTab.RUnlock()
	})

	go cc.rwLoop.LoopRead()
	go cc.rwLoop.LoopWrite()

	return cc, nil
}

func (c *Conn) write(msg tcp.Message) error {
	return c.rwLoop.Write(msg)
}

func (c *Conn) close() error {
	return c.rwLoop.Exit()
}

func (c *Conn) isClosed() bool {
	return !c.rwLoop.IsRunning()
}

type Client struct {
	name     string
	opaque   uint64
	connPool ConnPool
}

func NewGatewayClient(name string, addrProvider ServiceAddrProvider) *Client {
	addrProvider.Watch()

	return &Client{
		name: name,
		connPool: &ServiceConnPool{
			conns:    new(sync.Map),
			addrProv: addrProvider,
			new:      newConn,
		},
	}
}

func NewBackendClient(name string, address string, connNumPerHost int) *Client {
	if connNumPerHost <= 0 {
		connNumPerHost = 1
	}
	return &Client{
		name: name,
		connPool: &HostConnPool{
			conns:   make([]*Conn, connNumPerHost),
			size:    connNumPerHost,
			address: address,
			new:     newConn,
		},
	}
}

func (cli *Client) SyncRequest(ctx context.Context, request msg.Message) (msg.Message, error) {
	if request == nil {
		return nil, nil
	}

	opaque := atomic.AddUint64(&cli.opaque, 1)
	baudReq := tcp.Message{
		Opaque:  opaque,
		Message: request,
	}

	c, err := cli.connPool.GetConn()
	if err != nil {
		return nil, err
	}

	f := newFuture(opaque, nil)
	c.futureTab.add(opaque, f)
	defer c.futureTab.del(opaque)

	err = c.write(baudReq)
	if err != nil {
		cli.connPool.Destroy(c)
		return nil, err
	}

	resp, err := f.Get(ctx)
	//cli.connPool.PutConn(c)

	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (cli *Client) AsyncRequest(request msg.Message, callback Callback) error {
	if request == nil {
		return nil
	}

	opaque := atomic.AddUint64(&cli.opaque, 1)
	baudReq := tcp.Message{
		Opaque:  opaque,
		Message: request,
	}

	c, err := cli.connPool.GetConn()
	if err != nil {
		return err
	}

	if callback != nil {
		f := newFuture(opaque, callback)
		c.futureTab.add(opaque, f)
	}

	err = c.write(baudReq)
	if err != nil {
		if callback != nil {
			c.futureTab.del(opaque)
		}
		cli.connPool.Destroy(c)
		return err
	}

	//cli.connPool.PutConn(c)
	return nil
}

func (cli *Client) Close() (err error) {
	err = cli.connPool.Close()
	return
}

func (cli *Client) Name() string {
	return fmt.Sprintf("[Client]@[%s]", cli.name)
}
