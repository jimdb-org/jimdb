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
	"github.com/baudtime/baudtime/msg"
	"github.com/cespare/xxhash/v2"
	"net"
	"unsafe"
)

const sep = '\xff'

func YoloString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func YoloBytes(s string) []byte {
	return *(*[]byte)(unsafe.Pointer(&s))
}

func Ping(addr string) (ok bool) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return false
	}

	tcpConn, err := net.DialTCP("tcp4", nil, tcpAddr)
	if err != nil {
		return false
	}

	tcpConn.Close()
	return true
}

func Max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

type hasher struct {
	buf []byte
}

func NewHasher() *hasher {
	return &hasher{buf: make([]byte, 0, 1024)}
}

func (h *hasher) Hash(ls []msg.Label) uint64 {
	for _, v := range ls {
		h.buf = append(h.buf, v.Name...)
		h.buf = append(h.buf, sep)
		h.buf = append(h.buf, v.Value...)
		h.buf = append(h.buf, sep)
	}
	v := xxhash.Sum64(h.buf)
	h.buf = h.buf[:0]

	return v
}
