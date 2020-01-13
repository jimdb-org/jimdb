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

package errs

import (
	"errors"
	"fmt"
	"github.com/jimdb-org/jimdb/master/entity/pkg/mspb"
)

var codeErrMap = map[mspb.ErrorType]error{
	mspb.ErrorType_ClusterIDNotSame:         errors.New("clusterId not equal in Config"),
	mspb.ErrorType_NoSelectedNode:           errors.New("not selected node"),
	mspb.ErrorType_InternalError:            errors.New("internal error"),
	mspb.ErrorType_GenID:                    errors.New("gen ID failed"),
	mspb.ErrorType_DupDatabase:              errors.New("duplicate database"),
	mspb.ErrorType_DupTable:                 errors.New("duplicate table"),
	mspb.ErrorType_NotRuningTable:           errors.New("table is not runing"),
	mspb.ErrorType_NotExistDatabase:         errors.New("database not exist"),
	mspb.ErrorType_NotExistTable:            errors.New("table not exist"),
	mspb.ErrorType_NotExistNode:             errors.New("node not exist"),
	mspb.ErrorType_NotActiveNode:            errors.New("node is not up"),
	mspb.ErrorType_NotExistRange:            errors.New("range not exist"),
	mspb.ErrorType_ExistsRange:              errors.New("range exist"),
	mspb.ErrorType_NotExistPeer:             errors.New("range peer not exist"),
	mspb.ErrorType_NotEnoughResources:       errors.New("not enough resources"),
	mspb.ErrorType_InvalidParam:             errors.New("invalid param"),
	mspb.ErrorType_InvalidColumn:            errors.New("invalid column"),
	mspb.ErrorType_InvalidIndex:             errors.New("invalid index"),
	mspb.ErrorType_ColumnNameTooLong:        errors.New("column name is too long"),
	mspb.ErrorType_ColumnNotExist:           errors.New("column not exist"),
	mspb.ErrorType_DupColumnName:            errors.New("duplicate column name"),
	mspb.ErrorType_PkMustNotNull:            errors.New("primary key must be not nullable"),
	mspb.ErrorType_MissingPk:                errors.New("missing primary key"),
	mspb.ErrorType_PkMustNotSetDefaultValue: errors.New("primary key should not set default value"),
	mspb.ErrorType_NodeRejectNewPeer:        errors.New("node reject new peer"),
	mspb.ErrorType_NodeBlocked:              errors.New("node is blocked"),
	mspb.ErrorType_NodeStateConfused:        errors.New("confused node state"),
	mspb.ErrorType_NodeNotEnough:            errors.New("not enough node"),
	mspb.ErrorType_SchedulerExisted:         errors.New("scheduler is existed"),
	mspb.ErrorType_SchedulerNotFound:        errors.New("scheduler is not found"),
	mspb.ErrorType_WorkerExisted:            errors.New("worker is existed"),
	mspb.ErrorType_WorkerNotFound:           errors.New("worker is not found"),
	mspb.ErrorType_SqlReservedWord:          errors.New("sql reserved word"),
	mspb.ErrorType_SQLSyntaxError:           errors.New("syntax error"),
	mspb.ErrorType_RangeMetaConflict:        errors.New("range meta conflict"),
	mspb.ErrorType_NotFound:                 errors.New("entity not found"),
	mspb.ErrorType_NotAllowSplit:            errors.New("not allow split"),
	mspb.ErrorType_NotCancel:                errors.New("not allow cancel"),
	mspb.ErrorType_NotAllowDelete:           errors.New("not allow delete"),
	mspb.ErrorType_ClientIPNotSet:           errors.New("client ip not set"),
	mspb.ErrorType_DatabaseNotEmpty:         errors.New("database is not empty"),
	mspb.ErrorType_NodeStateNotExpected:     errors.New("node state not expected"),
	mspb.ErrorType_WatcherMasterHashErr:     errors.New("watcher master redirect has err"),
}

var errCodeMap = make(map[error]mspb.ErrorType)

func init() {
	for k, v := range codeErrMap {
		errCodeMap[v] = k
	}
}

func Error(errType mspb.ErrorType) (err error) {
	if err = codeErrMap[errType]; err != nil {
		return err
	}
	return fmt.Errorf("not found errType %d", errType)
}

func Code(err error) mspb.ErrorType {
	code := errCodeMap[err]
	if code == 0 {
		code = mspb.ErrorType_UnDefine
	}
	return code
}
