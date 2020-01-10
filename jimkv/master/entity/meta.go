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

package entity

import (
	"fmt"
	"github.com/jimdb-org/jimdb/jimkv/master/entity/errs"
	"github.com/jimdb-org/jimdb/jimkv/master/entity/pkg/mspb"
)

//ids sequence key for etcd
const (
	SequenceNodeID  = "/sequence/node/"
	SequenceRangeID = "/sequence/range/"
	SequencePeerID  = "/sequence/peer/"
	// new table or new index
	SequenceCreateID = "/sequence/create/"
)

const (
	PrefixLock        = "/lock/"
	PrefixLockCluster = "/lock/_cluster/"
	PrefixNode        = "/node/"
	PrefixNodeTTL     = "/ttl/node/"
	PrefixTable       = "/table/"
	PrefixRange       = "/range/"
	PrefixIndex       = "/index/"
	PrefixDataBase    = "/db/"
	PrefixCreate      = "/create/"
)

const (
	ConfAutoSplit = "/config/auto_split"
	ConfFailOver  = "/config/fail_over"
	ConfBalanced  = "/config/balanced"
)

//when master runing clean job , it will set value to this key,
//when other got key , now time less than this they will skip this job
const ClusterCleanJobKey = "/cluster/cleanjob"

func DBKey(id int32) string {
	return fmt.Sprintf("%sid/%d", PrefixDataBase, id)
}

func LockCluster() string {
	return fmt.Sprintf("%s", PrefixLockCluster)
}

func LockDBKey(dbID int32) string {
	return fmt.Sprintf("%s/%d", PrefixLockCluster, dbID)
}

func LockTableKey(dbID, tableID int32) string {
	return fmt.Sprintf("%s%d/%d", PrefixLock, dbID, tableID)
}

func NodeKey(nodeID uint64) string {
	return fmt.Sprintf("%s%d", PrefixNode, nodeID)
}

func NodeTTLKey(nodeID uint64) string {
	return fmt.Sprintf("%s%d", PrefixNodeTTL, nodeID)
}

func TableKey(dbID, tableID int32) string {
	return fmt.Sprintf("%s%d/%d", PrefixTable, dbID, tableID)
}

func TableKeyPre(dbID int32) string {
	return fmt.Sprintf("%s%d/", PrefixTable, dbID)
}

// include data/index type of ranges
// this is for compatibility
func RangeKey(tableID int32, rangeID uint64) string {
	return fmt.Sprintf("%s%d/%d", PrefixRange, tableID, rangeID)
}

// only include index type of ranges
func IndexRangeKey(tableID, indexID int32, rangeID uint64) string {
	return fmt.Sprintf("%s%d/%d/%d", PrefixIndex, tableID, indexID, rangeID)
}

func IndexRangeKeyPre(tableID, indexID int32) string {
	return fmt.Sprintf("%s%d/%d", PrefixIndex, tableID, indexID)
}

func RangeKeyPre(tableID int32) string {
	return fmt.Sprintf("%s%d/", PrefixRange, tableID)
}

func CreateKey(createID int64) string {
	return fmt.Sprintf("%s%d/", PrefixCreate, createID)
}

func OK() *mspb.ResponseHeader {
	return &mspb.ResponseHeader{ClusterId: Conf().Global.ClusterID}
}

func Err(e error) *mspb.ResponseHeader {
	if e == nil {
		e = errs.Error(mspb.ErrorType_UnDefine)
	}
	return &mspb.ResponseHeader{ClusterId: Conf().Global.ClusterID, Error: &mspb.Error{Code: uint32(errs.Code(e)), Message: e.Error()}}
}

type TableRecord struct {
	DbId       int32
	TableId    int32
	Replicas   int32
	CreateTime int64
	State      RecordState
}

type CreateRecord struct {
	CreateId    int64
	RngIds      []uint64
	CreateTime  int64
	CreateState RecordState
}

type RecordState int32

const (
	RecordState_Unknown RecordState = iota
	RecordState_Creating
	RecordState_Created
	RecordState_Failed
)
