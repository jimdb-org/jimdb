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
	"bufio"
	"errors"
	"fmt"
	"github.com/jimdb-org/jimdb/master/client/ds_client/util"
	"github.com/jimdb-org/jimdb/master/entity"
	"github.com/jimdb-org/jimdb/master/entity/pkg/dspb"
	"github.com/jimdb-org/jimdb/master/utils/log"
	"io"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gogo/protobuf/proto"
	"golang.org/x/net/context"
)

var maxMsgID uint64
var clientID int64

type RpcError struct {
	err error
}

func NewRpcError(err error) error {
	return &RpcError{err: err}
}

func (re *RpcError) Error() string {
	return re.err.Error()
}

const (
	TempSendQueueLen = 20
)

type MsgTypeGroup struct {
	req  uint16
	resp uint16
}

func (m *MsgTypeGroup) GetRequestMsgType() uint16 {
	return m.req
}

func (m *MsgTypeGroup) GetResponseMsgType() uint16 {
	return m.resp
}

var msgType map[uint16]*MsgTypeGroup

func init() {
	msgType = make(map[uint16]*MsgTypeGroup)

	msgType[uint16(dspb.FunctionID_kFuncSchedule)] = &MsgTypeGroup{0x01, 0x11}
	msgType[uint16(dspb.FunctionID_kFuncAdmin)] = &MsgTypeGroup{0x01, 0x11}
}

func getMsgType(funcId uint16) *MsgTypeGroup {
	if m, find := msgType[funcId]; find {
		return m
	}
	log.Error("invalid funcId %d", funcId)
	panic(fmt.Sprintf("invalid funcId %d", funcId))
	return nil
}

const (
	LINK_INIT = iota
	LINK_CONN
	LINK_CLOSED
	LINK_BAN_CONN
)

var (
	ErrClientClosed    = errors.New("client closed")
	ErrClientBusy      = errors.New("client is busy")
	ErrRequestTimeout  = errors.New("request timeout")
	ErrConnIdleTimeout = errors.New("conn idle timeout")
	ErrInvalidMessage  = errors.New("invalid message")
	ErrConnUnavailable = errors.New("the connection is unavailable")
	ErrConnClosing     = errors.New("the connection is closing")
	ErrNetworkIO       = errors.New("failed with network I/O error")
)

type (
	RpcClient interface {
		// admin
		CreateRange(ctx context.Context, in *dspb.CreateRangeRequest) (*dspb.SchResponse_Header, *dspb.CreateRangeResponse, error)
		DeleteRange(ctx context.Context, in *dspb.DeleteRangeRequest) (*dspb.SchResponse_Header, *dspb.DeleteRangeResponse, error)
		ChangeMember(ctx context.Context, request *dspb.ChangeRaftMemberRequest) (*dspb.SchResponse_Header, *dspb.ChangeRaftMemberResponse, error)
		TransferLeader(ctx context.Context, in *dspb.TransferRangeLeaderRequest) (*dspb.SchResponse_Header, *dspb.TransferRangeLeaderResponse, error)
		GetPeerInfo(ctx context.Context, in *dspb.GetPeerInfoRequest) (*dspb.SchResponse_Header, *dspb.GetPeerInfoResponse, error)
		IsAlive(ctx context.Context) (*dspb.SchResponse_Header, *dspb.IsAliveResponse, error)
		NodeInfo(ctx context.Context) (*dspb.SchResponse_Header, *dspb.NodeInfoResponse, error)
		Admin(ctx context.Context, in *dspb.AdminRequest) (*dspb.AdminResponse, error)
		Close()
	}
)

type Message struct {
	msgId     uint64
	msgType   uint16
	funcId    uint16
	flags     uint8
	protoType uint8
	// millisecond
	timeout uint32
	data    []byte

	done chan error
	ctx  context.Context
}

func (m *Message) GetMsgType() uint16 {
	return m.msgType
}

func (m *Message) SetMsgType(msgType uint16) {
	m.msgType = msgType
}

func (m *Message) GetFuncId() uint16 {
	return m.funcId
}

func (m *Message) SetFuncId(funcId uint16) {
	m.funcId = funcId
}

func (m *Message) GetMsgId() uint64 {
	return m.msgId
}

func (m *Message) SetMsgId(msgId uint64) {
	m.msgId = msgId
}

func (m *Message) GetFlags() uint8 {
	return m.flags
}

func (m *Message) SetFlags(flags uint8) {
	m.flags = flags
}

func (m *Message) GetProtoType() uint8 {
	return m.protoType
}

func (m *Message) SetProtoType(protoType uint8) {
	m.protoType = protoType
}

func (m *Message) GetTimeout() uint32 {
	return m.timeout
}

func (m *Message) SetTimeout(timeout uint32) {
	m.timeout = timeout
}

func (m *Message) GetData() []byte {
	return m.data
}

func (m *Message) SetData(data []byte) {
	m.data = data
}

func (m *Message) Back(err error) {
	select {
	case m.done <- err:
	default:
		log.Error("invalid message!!!!!")
	}
}

type List struct {
	lock sync.RWMutex
	list map[uint64]*Message
}

func NewList() *List {
	return &List{list: make(map[uint64]*Message)}
}

func (l *List) AddElement(m *Message) error {
	l.lock.Lock()
	defer l.lock.Unlock()
	if _, find := l.list[m.msgId]; find {
		return errors.New("element exist")
	}
	l.list[m.msgId] = m
	return nil
}

func (l *List) DelElement(id uint64) (*Message, bool) {
	l.lock.Lock()
	defer l.lock.Unlock()
	if m, find := l.list[id]; find {
		delete(l.list, id)
		return m, true
	}
	return nil, false
}

func (l *List) FindElement(id uint64) (*Message, bool) {
	l.lock.RLock()
	defer l.lock.RUnlock()
	if m, find := l.list[id]; find {
		return m, true
	}
	return nil, false
}

func (l *List) Cleanup(err error) {
	l.lock.Lock()
	defer l.lock.Unlock()
	for _, e := range l.list {
		select {
		case e.done <- err:
		default:
		}
	}
	// cleanup
	l.list = make(map[uint64]*Message)
}

func (l *List) Size() int {
	l.lock.RLock()
	defer l.lock.RUnlock()
	return len(l.list)

}

type WaitList struct {
	size     uint64
	waitList []*List
}

func NewWaitList(size int) *WaitList {
	waitList := make([]*List, size)
	for i := 0; i < size; i++ {
		waitList[i] = NewList()
	}
	return &WaitList{size: uint64(size), waitList: waitList}
}

func (wl *WaitList) AddElement(m *Message) error {
	index := m.msgId % wl.size
	return wl.waitList[index].AddElement(m)
}

func (wl *WaitList) DelElement(id uint64) (*Message, bool) {
	index := id % wl.size
	return wl.waitList[index].DelElement(id)
}

func (wl *WaitList) FindElement(id uint64) (*Message, bool) {
	index := id % wl.size
	return wl.waitList[index].FindElement(id)
}

func (wl *WaitList) Cleanup(err error) {
	for _, list := range wl.waitList {
		list.Cleanup(err)
	}
}

func (wl *WaitList) ElemSize() int {
	size := 0
	for _, list := range wl.waitList {
		size += list.Size()
	}
	return size
}

type DialFunc func(addr string) (*ConnTimeout, error)

var _ RpcClient = &DSRpcClient{}

type DSRpcClient struct {
	conn      *ConnTimeout
	connCount int
	writer    *bufio.Writer
	reader    *bufio.Reader
	// heartbeat
	//lastWriteTime time.Time
	sendQueue chan *Message
	waitList  *WaitList
	allowRecv chan bool
	addr      string
	dialFunc  DialFunc

	lock      sync.Mutex
	version   uint64
	linkState int
	//
	closed         bool
	heartbeatCount int64
	hbSendTime     int64
	clientId       int64
	ctx            context.Context
	cancel         context.CancelFunc
	wg             sync.WaitGroup
}

func NewDSRpcClient(addr string, dialFunc DialFunc) *DSRpcClient {
	ctx, cancel := context.WithCancel(context.Background())
	cli := &DSRpcClient{
		addr:      addr,
		dialFunc:  dialFunc,
		closed:    false,
		linkState: LINK_INIT,
		sendQueue: make(chan *Message, 100000),
		allowRecv: make(chan bool, 1),
		waitList:  NewWaitList(64),
		clientId:  atomic.AddInt64(&clientID, 1),
		ctx:       ctx, cancel: cancel}
	cli.wg.Add(1)
	go cli.sendLoop()
	cli.wg.Add(1)
	go cli.recvLoop()
	go cli.heartbeat()
	return cli
}

func (c *DSRpcClient) getMsgId() uint64 {
	return atomic.AddUint64(&maxMsgID, 1)
}

func (c *DSRpcClient) GetClientId() int64 {
	return c.clientId
}

func (c *DSRpcClient) execute(funId uint16, ctx context.Context, in proto.Message, out proto.Message) (uint64, error) {
	return c.executeWithFlags(funId, ctx, in, out, 0)
}

func (c *DSRpcClient) executeWithFlags(funId uint16, ctx context.Context, in proto.Message, out proto.Message, flags uint8) (uint64, error) {
	data, err := proto.Marshal(in)
	if err != nil {
		return 0, err
	}
	msgId := c.getMsgId()
	now := time.Now()
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(ReadTimeout)
	}
	timeout := deadline.Sub(now) / time.Millisecond
	if timeout <= 0 {
		return msgId, ErrRequestTimeout
	}
	message := &Message{
		done:    make(chan error, 1),
		msgId:   msgId,
		msgType: getMsgType(funId).GetRequestMsgType(),
		funcId:  uint16(funId),
		timeout: uint32(timeout),
		flags:   flags,
		ctx:     ctx,
		data:    data,
	}

	data, err = c.Send(ctx, message)
	if err != nil {
		return msgId, err
	}
	err = proto.Unmarshal(data, out)
	if err != nil {
		return msgId, err
	}
	return msgId, nil
}

func (c *DSRpcClient) ChangeMember(ctx context.Context, request *dspb.ChangeRaftMemberRequest) (*dspb.SchResponse_Header, *dspb.ChangeRaftMemberResponse, error) {
	out := new(dspb.SchResponse)
	_, err := c.execute(uint16(dspb.FunctionID_kFuncSchedule), ctx, &dspb.SchRequest{Header: &dspb.SchRequest_Header{ClusterId: entity.Conf().Global.ClusterID}, Req: &dspb.SchRequest_ChangeRaftMember{ChangeRaftMember: request}}, out)
	if err != nil {
		return nil, nil, err
	} else {
		return out.GetHeader(), out.GetChangeRaftMember(), nil
	}
}

func (c *DSRpcClient) IsAlive(ctx context.Context) (*dspb.SchResponse_Header, *dspb.IsAliveResponse, error) {
	out := new(dspb.SchResponse)

	deadline, cancelFunc := context.WithDeadline(ctx, time.Now().Add(5*time.Second))
	defer cancelFunc()

	_, err := c.execute(uint16(dspb.FunctionID_kFuncSchedule), deadline, &dspb.SchRequest{Header: &dspb.SchRequest_Header{ClusterId: entity.Conf().Global.ClusterID}, Req: &dspb.SchRequest_IsAlive{IsAlive: &dspb.IsAliveRequest{}}}, out)
	if err != nil {
		return nil, nil, err
	} else {
		return out.GetHeader(), out.GetIsAlive(), nil
	}
}

func (c *DSRpcClient) NodeInfo(ctx context.Context) (*dspb.SchResponse_Header, *dspb.NodeInfoResponse, error) {
	out := new(dspb.SchResponse)
	_, err := c.execute(uint16(dspb.FunctionID_kFuncSchedule), ctx, &dspb.SchRequest{Header: &dspb.SchRequest_Header{ClusterId: entity.Conf().Global.ClusterID}, Req: &dspb.SchRequest_NodeInfo{NodeInfo: &dspb.NodeInfoRequest{}}}, out)
	if err != nil {
		return nil, nil, err
	} else {
		return out.GetHeader(), out.GetNodeInfo(), nil
	}
}

func (c *DSRpcClient) CreateRange(ctx context.Context, in *dspb.CreateRangeRequest) (*dspb.SchResponse_Header, *dspb.CreateRangeResponse, error) {
	out := new(dspb.SchResponse)
	_, err := c.execute(uint16(dspb.FunctionID_kFuncSchedule), ctx, &dspb.SchRequest{Header: &dspb.SchRequest_Header{ClusterId: entity.Conf().Global.ClusterID}, Req: &dspb.SchRequest_CreateRange{CreateRange: in}}, out)
	if err != nil {
		return nil, nil, err
	} else {
		return out.Header, out.GetCreateRange(), nil
	}
}

func (c *DSRpcClient) DeleteRange(ctx context.Context, in *dspb.DeleteRangeRequest) (*dspb.SchResponse_Header, *dspb.DeleteRangeResponse, error) {
	out := new(dspb.SchResponse)
	_, err := c.execute(uint16(dspb.FunctionID_kFuncSchedule), ctx, &dspb.SchRequest{Header: &dspb.SchRequest_Header{ClusterId: entity.Conf().Global.ClusterID}, Req: &dspb.SchRequest_DeleteRange{DeleteRange: in}}, out)
	if err != nil {
		return nil, nil, err
	} else {
		return out.Header, out.GetDeleteRange(), nil
	}
}

func (c *DSRpcClient) TransferLeader(ctx context.Context, in *dspb.TransferRangeLeaderRequest) (*dspb.SchResponse_Header, *dspb.TransferRangeLeaderResponse, error) {
	out := new(dspb.SchResponse)
	_, err := c.execute(uint16(dspb.FunctionID_kFuncSchedule), ctx, &dspb.SchRequest{Header: &dspb.SchRequest_Header{ClusterId: entity.Conf().Global.ClusterID}, Req: &dspb.SchRequest_TransferRangeLeader{TransferRangeLeader: in}}, out)
	if err != nil {
		return nil, nil, err
	} else {
		return out.Header, out.GetTransferRangeLeader(), nil
	}
}

func (c *DSRpcClient) GetPeerInfo(ctx context.Context, in *dspb.GetPeerInfoRequest) (*dspb.SchResponse_Header, *dspb.GetPeerInfoResponse, error) {
	out := new(dspb.SchResponse)
	_, err := c.execute(uint16(dspb.FunctionID_kFuncSchedule), ctx, &dspb.SchRequest{Header: &dspb.SchRequest_Header{ClusterId: entity.Conf().Global.ClusterID}, Req: &dspb.SchRequest_GetPeerInfo{GetPeerInfo: in}}, out)
	if err != nil {
		return nil, nil, err
	} else {
		return out.Header, out.GetGetPeerInfo(), nil
	}
}

func (c *DSRpcClient) Admin(ctx context.Context, in *dspb.AdminRequest) (*dspb.AdminResponse, error) {
	out := new(dspb.AdminResponse)
	_, err := c.execute(uint16(dspb.FunctionID_kFuncAdmin), ctx, in, out)
	if err != nil {
		return nil, err
	} else {
		return out, nil
	}
}

func (c *DSRpcClient) Close() {
	if c.closed {
		return
	}
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.closed {
		return
	}
	c.linkState = LINK_BAN_CONN
	c.closed = true
	c.cancel()
	c.wg.Wait()
	if c.conn != nil {
		c.conn.Close()
	}
	c.waitList.Cleanup(ErrClientClosed)
}

func (c *DSRpcClient) Send(ctx context.Context, msg *Message) ([]byte, error) {
	metricSend := time.Now().UnixNano()
	// if linked err , so return
	var err error
	if c.closed {
		return nil, NewRpcError(ErrClientClosed)
	}
	if c.linkState != LINK_CONN {
		err = c.reconnect()
		if err != nil {
			return nil, err
		}
	}
	// add to wait queue
	select {
	case <-c.ctx.Done():
		return nil, NewRpcError(ErrClientClosed)
	case <-ctx.Done():
		return nil, NewRpcError(ErrRequestTimeout)
	case c.sendQueue <- msg:
	default:
		return nil, NewRpcError(ErrClientBusy)
	}
	sendToQ := (time.Now().UnixNano() - metricSend) / int64(time.Millisecond)
	if sendToQ > 2 {
		log.Warn("send queue to %s funId:%d  time:%d ms,msgid :%d ", c.addr, msg.funcId, sendToQ, msg.msgId)
	}

	select {
	case <-c.ctx.Done():
		err = ErrClientClosed
		log.Warn("request to %s client close funId:%d  time:%d ms,msgid :%d ", c.addr, msg.funcId, (time.Now().UnixNano()-metricSend)/int64(time.Millisecond), msg.msgId)
	case <-ctx.Done():
		err = ErrRequestTimeout
		log.Warn("request to %s timeout funId:%d  time:%d ms,msgid :%d ", c.addr, msg.funcId, (time.Now().UnixNano()-metricSend)/int64(time.Millisecond), msg.msgId)
	case err = <-msg.done:
	}
	c.waitList.DelElement(msg.msgId)
	if err != nil {
		return nil, err
	}

	sendDelay := (time.Now().UnixNano() - metricSend) / int64(time.Millisecond)
	if sendDelay <= 50 {
		// do nothing
	} else if sendDelay <= 200 {
		log.Info("clientId %d request to %s funId:%d execut time %d ms,msgid :%d", c.GetClientId(), c.addr, msg.funcId, sendDelay, msg.msgId)
	} else if sendDelay <= 500 {
		log.Warn("clientId %d request to %s funId:%d execut time %d ms,msgid :%d ", c.GetClientId(), c.addr, msg.funcId, sendDelay, msg.msgId)
	}

	return msg.GetData(), nil
}

func (c *DSRpcClient) reconnect() error {
	if c.linkState == LINK_BAN_CONN {
		return ErrClientClosed
	}
	version := c.version
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.linkState == LINK_BAN_CONN {
		return ErrClientClosed
	}
	// reconned
	if c.version > version && c.linkState == LINK_CONN {
		return nil
	}
	if c.linkState == LINK_CONN {
		if c.conn != nil {
			c.conn.Close()
		}
		c.linkState = LINK_CLOSED
	}
	c.connCount++
	c.heartbeatCount = 0
	conn, err := c.dialFunc(c.addr)
	if err == nil {
		c.conn = conn
		c.writer = bufio.NewWriterSize(conn, DefaultWriteSize)
		c.reader = bufio.NewReaderSize(conn, DefaultReadSize)
		c.linkState = LINK_CONN
		c.version += 1
		// start read
		select {
		case c.allowRecv <- true:
		default:
		}
		log.Info("dial %s successed", c.addr)
		return nil
	}
	log.Warn("dial %s failed, err[%v]", c.addr, err)
	return ErrConnUnavailable
}

func (c *DSRpcClient) heartbeat() {
	log.Info("start heartbeat %s clientId:%d", c.addr, c.GetClientId())
	defer func() {
		log.Warn("exit heartbeat %d %s", c.clientId, c.addr)
		if e := recover(); e != nil {
			log.Error("sendLoop err, [%v]", e)
			if _, flag := e.(error); flag {
				const size = 4096
				buf := make([]byte, size)
				buf = buf[:runtime.Stack(buf, false)]
				log.Error("sendLoop Run %s", string(buf))
			}
		}

	}()
	hbTicker := time.NewTicker(HeartbeatInterval)

	for {
		select {
		case <-c.ctx.Done():
			hbTicker.Stop()
			return
		case <-hbTicker.C:
			ctx, _ := context.WithTimeout(context.Background(), HeartbeatInterval)
			message := &Message{
				msgId:   c.getMsgId(),
				msgType: uint16(0x12),
				funcId:  uint16(dspb.FunctionID_kFuncHeartbeat),

				done: make(chan error, 1),
				ctx:  ctx,
			}

			curTime := time.Now().UnixNano()
			delay := (curTime - c.hbSendTime) / int64(HeartbeatInterval)
			if delay < 2 {
				//
			} else if delay < 3 {
				log.Warn("%s hb clientId %d delay too long %d ms", c.addr, c.GetClientId(), delay)
			} else if delay < 5 {
				log.Error("%s hb clientId %d delay too long %d ms", c.addr, c.GetClientId(), delay)
			}

			c.hbSendTime = curTime
			select {
			case c.sendQueue <- message:
			default:
			}
		}
	}

}

func (c *DSRpcClient) closeLink(err error) {
	c.waitList.Cleanup(err)
	if c.linkState != LINK_CONN {
		return
	}
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.linkState != LINK_CONN {
		return
	}
	c.linkState = LINK_CLOSED
	if c.conn != nil {
		if err != nil {
			log.Warn("clientId %d close conn:%s err:%s", c.GetClientId(), c.addr, err.Error())
		} else {
			log.Warn("clientId %d  close conn:%s err: nil", c.GetClientId(), c.addr)
		}

		c.conn.Close()
	}
}

func (c *DSRpcClient) sendLoop() {
	var message *Message
	var ok bool
	var err error
	//var messageQueue [TempSendQueueLen]*Message
	var queueCount int
	log.Info("start sendLoop %s clientId:%d", c.addr, c.GetClientId())
	defer func() {
		log.Warn("exit sendLoop %d %s", c.clientId, c.addr)
		if e := recover(); e != nil {
			log.Error("sendLoop err, [%v]", e)
			if _, flag := e.(error); flag {
				const size = 4096
				buf := make([]byte, size)
				buf = buf[:runtime.Stack(buf, false)]
				log.Error("sendLoop Run %s", string(buf))
			}
		}

	}()
	defer c.wg.Done()
	for {
		select {
		case <-c.ctx.Done():
			// clean all queue
			for {
				select {
				case message, ok = <-c.sendQueue:
					if !ok {
						return
					}
					message.Back(NewRpcError(ErrClientClosed))
				default:
					return
				}
			}
			return
		case message, ok = <-c.sendQueue:
			if !ok {
				log.Warn("%s %d send queue closed!!!!", c.addr, c.clientId)
				return
			}
			if c.closed {
				message.Back(NewRpcError(ErrClientClosed))
				return
			}
			//messageQueue[queueCount] = message
			c.sendMessage(message)
			queueCount++
			// reviced request package
		TryMore:
			for {
				select {
				case message, ok := <-c.sendQueue:
					if !ok {
						break TryMore
					}
					//messageQueue[queueCount] = message
					c.sendMessage(message)
					queueCount++
					if queueCount == TempSendQueueLen {
						break TryMore
					}
				default:
					break TryMore
				}
			}

			queueCount = 0
			// may be conn not success
			if c.writer != nil {
				err = c.writer.Flush()
				if err != nil {
					log.Warn("%d %s write message failed, err[%v]", c.clientId, c.addr, err)
					if err == io.EOF {
						err = ErrConnClosing
					} else {
						err = ErrNetworkIO
					}
					c.closeLink(err)
					//message.Back(err)
				}
			}
		}
	}
}

func (c *DSRpcClient) sendMessage(message *Message) {
	if c.linkState != LINK_CONN {
		err := c.reconnect()
		if err != nil {
			message.Back(NewRpcError(err))
			return
		}
	}

	c.waitList.AddElement(message)
	err := util.WriteMessage(c.writer, message)
	if err != nil {
		log.Warn("write message failed, err[%v]", err)
		if err == io.EOF {
			err = ErrConnClosing
		} else {
			err = ErrNetworkIO
		}
		message.Back(NewRpcError(err))
		// when send over to clean
		c.closeLink(err)
	}
}

func (c *DSRpcClient) recvWork() {
	var err error
	msg := &Message{}
	log.Info("start rpc client recv work %s clientId:%d", c.addr, c.GetClientId())

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		if c.linkState != LINK_CONN {
			err := c.reconnect()
			if err != nil {
				log.Error("reconn error clientId: %d %s", c.GetClientId(), c.addr)
				return
			}
		}
		err = util.ReadMessage(c.reader, msg)
		if err != nil {
			log.Warn("conn[%d] recv error %v", c.connCount, err)
			if err == io.EOF {
				err = ErrConnClosing
			} else if err == io.ErrUnexpectedEOF {
				err = ErrConnClosing
			} else if _e, ok := err.(net.Error); ok {
				if _e.Timeout() {
					log.Warn("must bug for read IO timeout")
					err = ErrRequestTimeout
				}
				//continue
			} else {
				err = ErrNetworkIO
			}
			c.closeLink(err)
			return
		}
		// for heart beat
		if msg.GetFuncId() != uint16(dspb.FunctionID_kFuncHeartbeat) {
			c.heartbeatCount = 0
			message, find := c.waitList.FindElement(msg.GetMsgId())
			if find {
				message.SetData(msg.GetData())
				message.Back(nil)
			} else {
				log.Warn("message[%d] timeout", msg.GetMsgId())
			}
		} else {
			c.heartbeatCount++
			c.waitList.DelElement(msg.GetMsgId())
			delay := (time.Now().UnixNano() - c.hbSendTime) / int64(time.Millisecond)
			if delay < 2 {
				//
			} else if delay < 5 {
				log.Warn("%s hb clientId %d response too long %d ms", c.addr, c.GetClientId(), delay)
			} else if delay < 10 {
				log.Error("%s hb clientId %d response too long %d ms", c.addr, c.GetClientId(), delay)
			}
			log.Debug("%s clientId %d delay %d ,wait msgId size :%d", c.addr, c.GetClientId(), delay, c.waitList.ElemSize())
			// time out , so break
			if time.Duration(c.heartbeatCount)*HeartbeatInterval > DefaultIdleTimeout {
				c.closeLink(ErrConnIdleTimeout)
				return
			}

		}
	}
}

func (c *DSRpcClient) recvLoop() {
	defer func() {
		log.Warn("exit recvWork %d %s", c.clientId, c.addr)
		if e := recover(); e != nil {
			log.Error("recvWork err, [%v]", e)
			if _, flag := e.(error); flag {
				const size = 4096
				buf := make([]byte, size)
				buf = buf[:runtime.Stack(buf, false)]
				log.Error("recvWork Run %s", string(buf))
			}
		}

	}()
	defer c.wg.Done()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-c.allowRecv:
			c.recvWork()
		}
	}
}
