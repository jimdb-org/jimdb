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
	"github.com/baudtime/baudtime/msg"
	"github.com/baudtime/baudtime/msg/backend"
	"github.com/baudtime/baudtime/msg/gateway"
	"sync"
)

const (
	//gateway
	GatewayAddRequestType MsgType = iota
	GatewayInstantQueryRequestType
	GatewayRangeQueryRequestType
	GatewayQueryResponseType
	GatewayLabelValuesRequestType
	//backend
	BackendAddRequestType
	BackendSelectRequestType
	BackendSelectResponseType
	BackendLabelValuesRequestType
	BackendSlaveOfCommandType
	BackendSyncHandshakeType
	BackendSyncHandshakeAckType
	BackendSyncHeartbeatType
	BackendSyncHeartbeatAckType
	BackendAdminCmdInfoType
	BackendAdminCmdJoinClusterType
	//other
	ConnCtrlType
	GeneralResponseType
	LabelValuesResponseType
)

var (
	gatewayAddRequestPool = sync.Pool{New: func() interface{} { return new(gateway.AddRequest) }}
	backendAddRequestPool = sync.Pool{New: func() interface{} { return new(backend.AddRequest) }}
)

func Type(m msg.Message) MsgType {
	switch m.(type) {
	//gateway
	case *gateway.AddRequest:
		return GatewayAddRequestType
	case *gateway.InstantQueryRequest:
		return GatewayInstantQueryRequestType
	case *gateway.RangeQueryRequest:
		return GatewayRangeQueryRequestType
	case *gateway.QueryResponse:
		return GatewayQueryResponseType
	case *gateway.LabelValuesRequest:
		return GatewayLabelValuesRequestType
	//backend
	case *backend.AddRequest:
		return BackendAddRequestType
	case *backend.SelectRequest:
		return BackendSelectRequestType
	case *backend.SelectResponse:
		return BackendSelectResponseType
	case *backend.LabelValuesRequest:
		return BackendLabelValuesRequestType
	case *backend.SlaveOfCommand:
		return BackendSlaveOfCommandType
	case *backend.SyncHandshake:
		return BackendSyncHandshakeType
	case *backend.SyncHandshakeAck:
		return BackendSyncHandshakeAckType
	case *backend.SyncHeartbeat:
		return BackendSyncHeartbeatType
	case *backend.SyncHeartbeatAck:
		return BackendSyncHeartbeatAckType
	case *backend.AdminCmdInfo:
		return BackendAdminCmdInfoType
	case *backend.AdminCmdJoinCluster:
		return BackendAdminCmdJoinClusterType
	//other
	case *msg.ConnCtrl:
		return ConnCtrlType
	case *msg.GeneralResponse:
		return GeneralResponseType
	case *msg.LabelValuesResponse:
		return LabelValuesResponseType
	}

	return BadMsgType
}

func Get(t MsgType) msg.Message {
	switch t {
	//gateway
	case GatewayAddRequestType:
		return gatewayAddRequestPool.Get().(*gateway.AddRequest)
	case GatewayInstantQueryRequestType:
		return new(gateway.InstantQueryRequest)
	case GatewayRangeQueryRequestType:
		return new(gateway.RangeQueryRequest)
	case GatewayQueryResponseType:
		return new(gateway.QueryResponse)
	case GatewayLabelValuesRequestType:
		return new(gateway.LabelValuesRequest)
	//backend
	case BackendAddRequestType:
		return backendAddRequestPool.Get().(*backend.AddRequest)
	case BackendSelectRequestType:
		return new(backend.SelectRequest)
	case BackendSelectResponseType:
		return new(backend.SelectResponse)
	case BackendLabelValuesRequestType:
		return new(backend.LabelValuesRequest)
	case BackendSlaveOfCommandType:
		return new(backend.SlaveOfCommand)
	case BackendSyncHandshakeType:
		return new(backend.SyncHandshake)
	case BackendSyncHandshakeAckType:
		return new(backend.SyncHandshakeAck)
	case BackendSyncHeartbeatType:
		return new(backend.SyncHeartbeat)
	case BackendSyncHeartbeatAckType:
		return new(backend.SyncHeartbeatAck)
	case BackendAdminCmdInfoType:
		return new(backend.AdminCmdInfo)
	case BackendAdminCmdJoinClusterType:
		return new(backend.AdminCmdJoinCluster)
	//other
	case ConnCtrlType:
		return new(msg.ConnCtrl)
	case GeneralResponseType:
		return new(msg.GeneralResponse)
	case LabelValuesResponseType:
		return new(msg.LabelValuesResponse)
	}

	return nil
}

func Put(m msg.Message) {
	switch req := m.(type) {
	case *gateway.AddRequest:
		gatewayAddRequestPool.Put(req)
	case *backend.AddRequest:
		backendAddRequestPool.Put(req)
	}
}
