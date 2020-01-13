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

package util

import (
	"encoding/binary"
	"fmt"
	"io"
)

const (
	msgHeaderSize        = 28
	msgVersion    uint16 = 1
	msgMagic      uint32 = 0x23232323
)

type Message interface {
	GetMsgType() uint16
	SetMsgType(uint16)
	GetFuncId() uint16
	SetFuncId(uint16)
	GetMsgId() uint64
	SetMsgId(uint64)
	GetFlags() uint8
	SetFlags(uint8)
	GetProtoType() uint8
	SetProtoType(uint8)
	GetTimeout() uint32
	SetTimeout(uint32)
	GetData() []byte
	SetData([]byte)
}

// The RPC format is header + protocol buffer body
// Header is 24 bytes, format:
//  | magic(4 bytes magic value, offset 0) | version(2 bytes, offset 4) |
//  | MsgType(2 bytes, offset 6) | FuncID(2 bytes, offset 8) | MsgID(8 bytes, offset 10) |
//  | Stream hash(1 bytes) | proto type(1 bytes) | time out(4 bytes) | msg_len(4 bytes) |,
// all use bigendian.

// WriteMessage writes a protocol buffer message to writer.
func WriteMessage(w io.Writer, msg Message) error {
	var header [msgHeaderSize]byte
	// magic value 4 bytes
	binary.BigEndian.PutUint32(header[0:4], msgMagic)
	// version     2 bytes
	binary.BigEndian.PutUint16(header[4:6], msgVersion)
	// msg type    2 bytes [first byte 0: req 1:　rep   secode byte 1:　control 2: data]
	binary.BigEndian.PutUint16(header[6:8], msg.GetMsgType())
	// func ID     2 bytes
	binary.BigEndian.PutUint16(header[8:10], msg.GetFuncId())
	// msg ID      8 bytes
	binary.BigEndian.PutUint64(header[10:18], msg.GetMsgId())
	// Stream hash 1 bytes
	header[18] = byte(msg.GetFlags())
	// proto type  1 bytes  [0: protobuf 1: json ......]
	header[19] = byte(msg.GetProtoType())
	// time out     4 bytes
	binary.BigEndian.PutUint32(header[20:24], msg.GetTimeout())
	// msg len     4 bytes
	binary.BigEndian.PutUint32(header[24:28], uint32(len(msg.GetData())))
	if _, err := w.Write(header[:]); err != nil {
		return err
	}

	_, err := w.Write(msg.GetData())
	return err
}

// ReadMessage reads a protocol buffer message from reader.
func ReadMessage(r io.Reader, msg Message) (err error) {
	var header [msgHeaderSize]byte
	_, err = io.ReadFull(r, header[:])
	if err != nil {
		return
	}
	magic := binary.BigEndian.Uint32(header[0:4])
	if magic != msgMagic {
		err = fmt.Errorf("mismatch header magic %x != %x", magic, msgMagic)
		return
	}
	// skip version now. 4:6
	msg.SetMsgType(binary.BigEndian.Uint16(header[6:8]))
	msg.SetFuncId(binary.BigEndian.Uint16(header[8:10]))
	msg.SetMsgId(binary.BigEndian.Uint64(header[10:18]))
	msgLen := binary.BigEndian.Uint32(header[24:28])
	data := make([]byte, msgLen)
	_, err = io.ReadFull(r, data)
	if err != nil {
		return
	}
	msg.SetData(data)

	return
}
