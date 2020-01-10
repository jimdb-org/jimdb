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
	"encoding/binary"
	"github.com/baudtime/baudtime/msg"
	"github.com/pkg/errors"
)

type Message struct {
	msg.Message
	Opaque uint64
}

func (msg *Message) GetOpaque() uint64 {
	return msg.Opaque
}

func (msg *Message) SetOpaque(opaque uint64) {
	msg.Opaque = opaque
}

func (msg *Message) GetRaw() msg.Message {
	return msg.Message
}

func (msg *Message) SetRaw(raw msg.Message) {
	msg.Message = raw
}

func (msg *Message) SizeOfRaw() int {
	if msg.Message != nil {
		return msg.Message.Msgsize()
	}
	return 0
}

type MsgType uint8

const BadMsgType MsgType = 255

var (
	EmptyMsg        = Message{}
	MsgSizeOverflow = errors.New("message size overflow")
	BadMsgFormat    = errors.New("bad message format")
)

type MsgCodec struct{}

func (codec *MsgCodec) Encode(msg Message, b []byte) (int, error) {
	raw := msg.GetRaw()
	written := 0

	b[written] = byte(Type(raw))
	written++

	n := binary.PutUvarint(b[written:written+binary.MaxVarintLen64], msg.Opaque)
	written += n

	if raw != nil {
		if cap(b)-written < raw.Msgsize() {
			return 0, MsgSizeOverflow
		}

		o, err := raw.MarshalMsg(b[written:written])
		if err != nil {
			return 0, err
		}
		written += len(o)
	}

	return written, nil
}

func (codec *MsgCodec) Decode(b []byte) (Message, error) {
	var (
		err error
		msg Message
	)

	//get message type
	msgType := MsgType(b[0])

	//get message opaque
	l := 1 + binary.MaxVarintLen64
	if len(b) < l {
		l = len(b)
	}
	opaque, n := binary.Uvarint(b[1:l])
	if n <= 0 {
		return msg, BadMsgFormat
	}

	//get message proto
	raw := Get(msgType)
	if raw != nil {
		_, err = raw.UnmarshalMsg(b[1+n:])
	}

	if err != nil {
		return msg, err
	}

	msg.SetOpaque(opaque)
	msg.SetRaw(raw)

	return msg, nil
}
