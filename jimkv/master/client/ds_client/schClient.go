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
	"fmt"
	"github.com/jimdb-org/jimdb/jimkv/master/entity"
	"github.com/jimdb-org/jimdb/jimkv/master/entity/pkg/basepb"
	"github.com/jimdb-org/jimdb/jimkv/master/entity/pkg/dspb"
	"github.com/jimdb-org/jimdb/jimkv/master/utils/log"
	"golang.org/x/net/context"
	"time"
)

type (
	// Client is a client that sends RPC.
	// It should not be used after calling Close().
	SchClient interface {
		// Close should release all data.
		Close() error
		// SendKVReq sends kv request.
		CreateRange(ctx context.Context, addr string, r *basepb.Range) error
		DeleteRange(ctx context.Context, addr string, rangeId uint64, peerID uint64) error
		ChangeMember(ctx context.Context, addr string, r *dspb.ChangeRaftMemberRequest) error
		TransferLeader(ctx context.Context, addr string, rangeId uint64) error
		GetPeerInfo(addr string, rangeId uint64) (*dspb.GetPeerInfoResponse, error)
		IsAlive(ctx context.Context, addr string) bool
		NodeInfo(ctx context.Context, addr string) (*dspb.NodeInfoResponse, error)
	}
)

const DefLog = 1

type SchRpcClient struct {
	pool *ResourcePool
}

func (c *SchRpcClient) ChangeMember(ctx context.Context, addr string, r *dspb.ChangeRaftMemberRequest) (err error) {
	var conn RpcClient
	var header *dspb.SchResponse_Header
	now := time.Now()
	defer func() {
		log.GetrDef(DefLog).Info("to change member addr:[%s] rangeID:[%d] changeType:[%v] peerID:[%d] useTime:[%s]", addr, r.RangeId, r.ChangeType, r.TargetPeer.Id, time.Now().Sub(now))
		m := entity.Monitor()
		if m != nil {
			m.GetGauge(m.GetCluster(), "schedule", "count").Add(1)
			if err != nil {
				m.GetGauge(m.GetCluster(), "schedule", "event", "change_member", "fail").Add(1)
			} else {
				m.GetGauge(m.GetCluster(), "schedule", "event", "change_member", "success").Add(1)
			}
		}
	}()

	conn, err = c.getConn(addr)
	if err != nil {
		log.GetrDef(DefLog).Error("is alive has err:[%s]", err.Error())
		return err
	}

	header, _, err = conn.ChangeMember(ctx, r)
	if err != nil {
		log.GetrDef(DefLog).Error("is alive has err:[%s]", err.Error())
		return err
	}
	if header.GetError() != nil {
		err = fmt.Errorf("SchRpcClient ChangeMember hash err:[%s]", header.GetError().String())
		log.GetrDef(DefLog).Error(err.Error())
		return err
	}
	return nil
}

func (c *SchRpcClient) NodeInfo(ctx context.Context, addr string) (*dspb.NodeInfoResponse, error) {
	conn, err := c.getConn(addr)
	if err != nil {
		return nil, err
	}

	header, response, err := conn.NodeInfo(ctx)
	if err != nil {
		log.GetrDef(DefLog).Error("is alive has err:[%s]", err.Error())
		return nil, err
	}
	if header.GetError() != nil {
		return nil, fmt.Errorf("SchRpcClient NodeInfo hash err:[%s]", header.GetError().String())
	}
	return response, nil
}

func (c *SchRpcClient) IsAlive(ctx context.Context, addr string) bool {
	conn, err := c.getConn(addr)
	if err != nil {
		return false
	}

	header, response, err := conn.IsAlive(ctx)
	if err != nil {
		log.Error("is alive has err:[%s]", err.Error())
		return false
	}
	if header.GetError() != nil {
		log.Error("check isAlive has err in header:[%s]", header.GetError().String())
		return false
	}
	return response.Alive

}

func NewSchRPCClient(size int) SchClient {
	if size <= 0 {
		size = DefaultPoolSize
	}
	return &SchRpcClient{pool: NewResourcePool(size)}
}

func (c *SchRpcClient) Close() error {
	c.pool.Close()
	return nil
}

func (c *SchRpcClient) CreateRange(ctx context.Context, addr string, r *basepb.Range) (err error) {
	var conn RpcClient
	var header *dspb.SchResponse_Header

	now := time.Now()
	defer func() {
		go log.GetrDef(DefLog).Info("create Range addr:[%s] range:[%d] useTime:[%s]", addr, r.Id, time.Now().Sub(now))
		m := entity.Monitor()
		if m != nil {
			m.GetGauge(m.GetCluster(), "schedule", "count").Add(1)
			if err != nil {
				m.GetGauge(m.GetCluster(), "schedule", "event", "add_peer", "fail").Add(1)
			} else {
				m.GetGauge(m.GetCluster(), "schedule", "event", "add_peer", "success").Add(1)
			}
		}
	}()

	conn, err = c.getConn(addr)
	if err != nil {
		log.GetrDef(DefLog).Error(err.Error())
		return err
	}
	req := &dspb.CreateRangeRequest{
		Range:  r,
		Leader: r.Peers[0].NodeId,
	}

	header, _, err = conn.CreateRange(ctx, req)
	if err != nil {
		err = fmt.Errorf("addr[%s] SchRpcClient CreateRange hash err:[%s]", addr, err.Error())
		log.GetrDef(DefLog).Error(err.Error())
		return err
	}

	if header.GetError() != nil {
		err = fmt.Errorf("addr[%s] SchRpcClient CreateRange hash err:[%s]", addr, header.GetError().String())
		log.GetrDef(DefLog).Error(err.Error())
		return err
	}
	return nil
}

func (c *SchRpcClient) DeleteRange(ctx context.Context, addr string, rangeId uint64, peerID uint64) (err error) {
	var conn RpcClient
	var header *dspb.SchResponse_Header
	now := time.Now()
	defer func() {
		go log.GetrDef(DefLog).Info("delete Range addr:[%s] range:[%d] peer:[%d] useTime:[%s]", addr, rangeId, peerID, time.Now().Sub(now))
		m := entity.Monitor()
		if m != nil {
			m.GetGauge(m.GetCluster(), "schedule", "count").Add(1)
			if err != nil {
				m.GetGauge(m.GetCluster(), "schedule", "event", "del_peer", "fail").Add(1)
			} else {
				m.GetGauge(m.GetCluster(), "schedule", "event", "del_peer", "success").Add(1)
			}
		}
	}()

	conn, err = c.getConn(addr)
	if err != nil {
		log.GetrDef(DefLog).Error(err.Error())
		return err
	}
	req := &dspb.DeleteRangeRequest{RangeId: rangeId, PeerId: peerID}
	header, _, err = conn.DeleteRange(ctx, req)
	if err != nil {
		err = fmt.Errorf("SchRpcClient DeleteRange hash err:[%s]", err.Error())
		log.GetrDef(DefLog).Error(err.Error())
		return err
	}
	if header.GetError() != nil {
		err = fmt.Errorf("SchRpcClient DeleteRange hash head err:[%s]", header.GetError().String())
		log.GetrDef(DefLog).Error(err.Error())
		return err
	}
	return nil
}

func (c *SchRpcClient) TransferLeader(ctx context.Context, addr string, rangeId uint64) (err error) {
	var conn RpcClient
	var header *dspb.SchResponse_Header
	now := time.Now()
	defer func() {
		go log.GetrDef(DefLog).Info("transfer leader addr:[%s] range:[%d] useTime:[%s]", addr, rangeId, time.Now().Sub(now))
		m := entity.Monitor()
		if m != nil {
			m.GetGauge(m.GetCluster(), "schedule", "count").Add(1)
			if err != nil {
				m.GetGauge(m.GetCluster(), "schedule", "event", "transfer_leader", "fail").Add(1)
			} else {
				m.GetGauge(m.GetCluster(), "schedule", "event", "transfer_leader", "success").Add(1)
			}
		}
	}()

	conn, err = c.getConn(addr)
	if err != nil {
		return err
	}
	req := &dspb.TransferRangeLeaderRequest{RangeId: rangeId}
	header, _, err = conn.TransferLeader(ctx, req)
	if err != nil {
		err = fmt.Errorf("SchRpcClient TransferLeader hash err:[%s]", err.Error())
		log.GetrDef(DefLog).Error(err.Error())
		return err
	}
	if header.GetError() != nil {
		err = fmt.Errorf("SchRpcClient TransferLeader hash err:[%s]", header.GetError().String())
		log.GetrDef(DefLog).Error(err.Error())
		return err
	}
	return nil
}

func (c *SchRpcClient) GetPeerInfo(addr string, rangeId uint64) (*dspb.GetPeerInfoResponse, error) {
	conn, err := c.getConn(addr)
	if err != nil {
		return nil, err
	}
	req := &dspb.GetPeerInfoRequest{
		RangeId: rangeId,
	}
	ctx, cancel := context.WithTimeout(context.Background(), ReadTimeoutShort)
	defer cancel()
	header, resp, err := conn.GetPeerInfo(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("SchRpcClient GetPeerInfo hash err:[%s]", err.Error())
	}
	if header.GetError() != nil {
		return nil, fmt.Errorf("SchRpcClient GetPeerInfo hash err:[%s]", header.GetError().String())
	}
	return resp, nil
}

func (c *SchRpcClient) getConn(addr string) (RpcClient, error) {
	if len(addr) == 0 {
		return nil, errors.New("invalid address")
	}
	return c.pool.GetConn(addr)
}
