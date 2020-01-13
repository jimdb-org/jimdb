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

package tcp

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"sync/atomic"

	"github.com/baudtime/baudtime/msg"
	"github.com/baudtime/baudtime/util/syn"
	. "github.com/baudtime/baudtime/vars"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
)

var bytesPool = syn.NewBucketizedPool(1e3, 1e7, 4, false, func(s int) interface{} { return make([]byte, s) }, func() syn.Bucket {
	return new(sync.Pool)
})

type ReadWriteLoop struct {
	conn     *Conn
	codec    MsgCodec
	out      *syn.Queue
	handle   func(ctx context.Context, in Message, inBytes []byte) Message
	rdClosed uint32
	wrClosed uint32
	closed   uint32
	onExit   func()
}

func (loop *ReadWriteLoop) LoopWrite() {
	block := true

	for loop.IsRunning() && !loop.WriteClosed() {
		msgV := loop.out.Dequeue(block)

		if msgV != nil {
			bytes, ok := msgV.([]byte)
			if !ok {
				continue
			}

			err := loop.conn.WriteMsg(bytes)
			bytesPool.Put(bytes)
			if err != nil {
				if _, ok := err.(net.Error); ok || err == io.EOF || err == io.ErrUnexpectedEOF {
					loop.Exit()
					return
				}

				level.Error(Logger).Log("msg", "write loop responsing client failed", "err", err)
			}

			block = false
		} else if !block {
			loop.conn.Flush()
			block = true
		}
	}
}

func (loop *ReadWriteLoop) LoopRead() {
	ctx := context.Background()

	for loop.IsRunning() && !loop.ReadClosed() {
		inBytes, err := loop.conn.ReadMsg()
		if err != nil {
			if _, ok := err.(net.Error); ok || err == io.EOF || err == io.ErrUnexpectedEOF {
				loop.Exit()
				return
			}

			level.Error(Logger).Log("msg", "read loop reading request failed", "err", err)
			continue
		}

		in, err := loop.codec.Decode(inBytes)
		if err != nil {
			level.Error(Logger).Log("msg", "decode err", "err", err)
			loop.Exit()
			return
		}

		if connCtrl, ok := in.Message.(*msg.ConnCtrl); ok {
			switch connCtrl.Code {
			case msg.CtrlCode_CloseRead:
				err = loop.CloseRead()
			case msg.CtrlCode_CloseWrite:
				err = loop.CloseWrite()
			}
			level.Info(Logger).Log("connCtrl", connCtrl.Code, "err", err)
			continue
		}

		out := loop.handle(ctx, in, inBytes)
		Put(in.GetRaw())

		if loop.WriteClosed() || out == EmptyMsg {
			continue
		}

		outBytes := bytesPool.Get(1 + binary.MaxVarintLen64 + out.SizeOfRaw()).([]byte)
		n, err := loop.codec.Encode(out, outBytes)
		if err != nil {
			level.Error(Logger).Log("msg", "encode err", "err", err)
			continue
		}

		loop.out.Enqueue(outBytes[:n])
	}
}

func (loop *ReadWriteLoop) Write(msg Message) error {
	if !loop.IsRunning() {
		return errors.New("loop is not running")
	}

	if loop.WriteClosed() {
		return errors.New("write is closed")
	}

	bytes := bytesPool.Get(1 + binary.MaxVarintLen64 + msg.SizeOfRaw()).([]byte)
	n, err := loop.codec.Encode(msg, bytes)
	if err != nil {
		bytesPool.Put(bytes)
		return err
	}

	err = loop.out.Enqueue(bytes[:n])
	if err != nil {
		bytesPool.Put(bytes)
	}

	return err
}

func (loop *ReadWriteLoop) CloseWrite() (err error) {
	if atomic.CompareAndSwapUint32(&loop.wrClosed, 0, 1) {
		err = loop.conn.CloseWrite()
		loop.out.Close()
	}
	return
}

func (loop *ReadWriteLoop) WriteClosed() bool {
	return atomic.LoadUint32(&loop.wrClosed) == 1
}

func (loop *ReadWriteLoop) CloseRead() (err error) {
	if atomic.CompareAndSwapUint32(&loop.rdClosed, 0, 1) {
		err = loop.conn.CloseRead()
	}
	return
}

func (loop *ReadWriteLoop) ReadClosed() bool {
	return atomic.LoadUint32(&loop.rdClosed) == 1
}

func (loop *ReadWriteLoop) Exit() (err error) {
	if atomic.CompareAndSwapUint32(&loop.closed, 0, 1) {
		loop.conn.Flush()
		err = loop.conn.Close()
		loop.out.Close()

		if loop.onExit != nil {
			loop.onExit()
		}
	}
	return
}

func (loop *ReadWriteLoop) OnExit(f func()) {
	loop.onExit = f
}

func (loop *ReadWriteLoop) IsRunning() bool {
	return atomic.LoadUint32(&loop.closed) == 0
}

func NewReadWriteLoop(conn *net.TCPConn, handle func(ctx context.Context, in Message, inBytes []byte) Message) *ReadWriteLoop {
	return &ReadWriteLoop{
		conn:   NewConn(conn),
		out:    syn.NewQueue(1024 * 8),
		handle: handle,
	}
}
