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

package service

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/jimdb-org/jimdb/jimkv/master/client/ds_client"
	"github.com/jimdb-org/jimdb/jimkv/master/client/store"
	"github.com/jimdb-org/jimdb/jimkv/master/entity"
	"github.com/jimdb-org/jimdb/jimkv/master/entity/errs"
	"github.com/jimdb-org/jimdb/jimkv/master/entity/pkg/basepb"
	"github.com/jimdb-org/jimdb/jimkv/master/entity/pkg/mspb"
	"github.com/jimdb-org/jimdb/jimkv/master/utils/cblog"
	"github.com/jimdb-org/jimdb/jimkv/master/utils/log"
	"github.com/spf13/cast"
	"strconv"
)

func NewBaseService() (*BaseService, error) {
	openStore, err := store.OpenStore("etcd", entity.Conf().Masters.ClientAddress())
	if err != nil {
		return nil, cblog.LogErrAndReturn(err)
	}
	return &BaseService{
		Store:     openStore,
		dsClient:  client.NewSchRPCClient(1),
		admClient: client.NewAdminClient("", 1),
	}, nil
}

type BaseService struct {
	store.Store
	dsClient  client.SchClient
	admClient client.AdminClient
}

func (bs *BaseService) QueryNode(ctx context.Context, nodeID uint64) (*basepb.Node, error) {
	nodeBytes, err := bs.Get(ctx, entity.NodeKey(nodeID))
	if err != nil {
		return nil, cblog.LogErrAndReturn(err)
	}
	if nodeBytes == nil {
		return nil, errs.Error(mspb.ErrorType_NotExistNode)
	}

	node := &basepb.Node{}
	if err := node.Unmarshal(nodeBytes); err != nil {
		return nil, cblog.LogErrAndReturn(err)
	}
	return node, nil
}

func (bs *BaseService) QueryAllNodes(ctx context.Context) ([]*basepb.Node, error) {
	_, value, err := bs.PrefixScan(ctx, entity.PrefixNode)
	if err != nil {
		return nil, cblog.LogErrAndReturn(err)
	}

	nodes := make([]*basepb.Node, 0, len(value))

	for _, nodeByte := range value {
		node := &basepb.Node{}
		if err := node.Unmarshal(nodeByte); err != nil {
			return nil, cblog.LogErrAndReturn(err)
		}
		nodes = append(nodes, node)
	}

	return nodes, nil

}

func (bs *BaseService) QueryOnlineNodes(ctx context.Context) ([]*basepb.Node, error) {
	_, value, err := bs.PrefixScan(ctx, entity.PrefixNodeTTL)
	if err != nil {
		return nil, cblog.LogErrAndReturn(err)
	}

	nodes := make([]*basepb.Node, 0, len(value))

	for _, nodeIDByte := range value {
		node, err := bs.GetNode(ctx, cast.ToUint64(string(nodeIDByte)))
		if err != nil {
			log.Error("query online node by:[%s] id err:[%s]", string(nodeIDByte), err.Error())
		} else {
			nodes = append(nodes, node)
		}

	}

	return nodes, nil

}

func (bs *BaseService) QueryTablesByDBID(ctx context.Context, dbID int32) ([]*entity.TableRecord, error) {
	_, values, e := bs.PrefixScan(ctx, entity.TableKeyPre(dbID))
	if e != nil {
		return nil, e
	}

	tables := make([]*entity.TableRecord, len(values))

	for i := range values {
		tables[i] = &entity.TableRecord{}
		err := json.Unmarshal(values[i], tables[i])
		if err != nil {
			return nil, err
		}
	}

	return tables, nil
}

func (bs *BaseService) QueryTableByID(ctx context.Context, dbID, tableID int32) (*entity.TableRecord, error) {
	tableBytes, err := bs.Get(ctx, entity.TableKey(dbID, tableID))
	if err != nil {
		return nil, cblog.LogErrAndReturn(err)
	}
	if tableBytes == nil {
		return nil, errs.Error(mspb.ErrorType_NotExistTable)
	}

	table := &entity.TableRecord{}
	if err = json.Unmarshal(tableBytes, table); err != nil {
		return nil, cblog.LogErrAndReturn(err)
	}
	return table, nil
}

func (bs *BaseService) QueryDatabases(ctx context.Context) ([]int32, error) {
	_, value, err := bs.PrefixScan(ctx, fmt.Sprintf("%sid/", entity.PrefixDataBase))
	if err != nil {
		return nil, cblog.LogErrAndReturn(err)
	}

	dbs := make([]int32, 0, len(value))

	for _, dbByte := range value {
		db := cast.ToInt32(string(dbByte))
		dbs = append(dbs, db)
	}

	return dbs, nil

}

func (bs *BaseService) QueryDBByID(ctx context.Context, dbID int32) (int32, error) {
	dbBytes, err := bs.Get(ctx, entity.DBKey(dbID))
	if err != nil {
		return -1, cblog.LogErrAndReturn(err)
	}
	if dbBytes == nil {
		return -1, errs.Error(mspb.ErrorType_NotExistDatabase)
	}

	var i int
	if i, err = strconv.Atoi(string(dbBytes)); err != nil {
		return -1, cblog.LogErrAndReturn(err)
	}

	return int32(i), nil
}

func (bs *BaseService) QueryRange(ctx context.Context, tableID int32, rangeID uint64) (*basepb.Range, error) {
	rangeBytes, err := bs.Get(ctx, entity.RangeKey(tableID, rangeID))
	if err != nil {
		return nil, cblog.LogErrAndReturn(err)
	}
	if rangeBytes == nil {
		return nil, errs.Error(mspb.ErrorType_NotExistRange)
	}

	base := &basepb.Range{}

	if err := base.Unmarshal(rangeBytes); err != nil {
		return nil, cblog.LogErrAndReturn(err)
	}

	return base, nil
}

// if tableID ==0 it will return all ranges , if table not exist it not return any error
func (bs *BaseService) QueryRanges(ctx context.Context, tableID, indexID int32) ([]*basepb.Range, error) {

	var (
		values [][]byte
		e      error
	)

	if tableID == 0 {
		_, values, e = bs.PrefixScan(ctx, entity.PrefixRange)
	} else if tableID > 0 && indexID <= 0 {
		_, values, e = bs.PrefixScan(ctx, entity.RangeKeyPre(tableID))
	} else if tableID > 0 && indexID > 0 {
		_, values, e = bs.PrefixScan(ctx, entity.IndexRangeKeyPre(tableID, indexID))
	}

	if e != nil {
		return nil, e
	}

	ranges := make([]*basepb.Range, len(values))

	for i := range values {
		ranges[i] = &basepb.Range{}
		if e := ranges[i].Unmarshal(values[i]); e != nil {
			return nil, e
		}
	}

	return ranges, nil
}
