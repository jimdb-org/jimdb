// Copyright 2019 The JimDB Authors
// Unless required by applicable law or agreed to in writing, software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package schedule

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/jimdb-org/jimdb/jimkv/master/entity"
	"github.com/jimdb-org/jimdb/jimkv/master/entity/pkg/basepb"
	"github.com/jimdb-org/jimdb/jimkv/master/service"
	"github.com/jimdb-org/jimdb/jimkv/master/utils/cblog"
	"github.com/spf13/cast"
	"time"

	"github.com/jasonlvhit/gocron"

	"github.com/jimdb-org/jimdb/jimkv/master/utils/log"
	"go.etcd.io/etcd/clientv3/concurrency"
	"runtime"
)

type processContext struct {
	context.Context
	cancel          context.CancelFunc
	stop            bool
	nodeMap         map[uint64]*basepb.Node
	nodeHandlerMap  service.NodeHandlerMap
	rangeMap        map[uint64]*basepb.Range
	tableMap        map[int32]*entity.TableRecord
	dbMap           map[int32]int32 // dbId - dbId
	createRecordMap map[int64]*entity.CreateRecord
}

type ProcessJob interface {
	process(ctx *processContext)
}

var skipJob = fmt.Errorf("skip job")

func process(mCtx context.Context, service *service.BaseService, jobs []ProcessJob) {

	ctx := &processContext{}

	ctx.Context, ctx.cancel = context.WithTimeout(mCtx, 5*time.Minute)

	defer func() {
		if err := recover(); err != nil {
			log.Error("!!!!!!!!!!!System ERROR: " + cblog.LogErrAndReturn(fmt.Errorf(cast.ToString(err))).Error())
			printStack()
		}
		select {
		case err := <-ctx.Done():
			log.Error("process job stop timeout is [%d]s so cancel it info:[%v]", entity.Conf().Global.ScheduleSecond, err)
		default:
			ctx.cancel()
		}
	}()

	scheduleTime := int64(entity.Conf().Global.ScheduleSecond) * int64(time.Second)

	var err = service.STM(ctx, func(stm concurrency.STM) error {
		timeBytes := stm.Get(entity.ClusterCleanJobKey)

		var value uint64

		if len(timeBytes) == 0 {
			value = 0
		} else {
			value = binary.LittleEndian.Uint64([]byte(timeBytes))
		}

		if time.Now().UnixNano() > int64(value) {
			bytes := make([]byte, 8)
			binary.LittleEndian.PutUint64(bytes, uint64(time.Now().UnixNano()+scheduleTime))
			stm.Put(entity.ClusterCleanJobKey, string(bytes))
			return nil
		}
		return skipJob
	})
	if err == skipJob {
		log.Info("skip schedule task .....")
		// filter monitor job for reset cluster metrics
		for _, job := range jobs {
			switch m := job.(type) {
			case *MonitorJob:
				m.reset(ctx)
			default:
				break
			}
		}
		return
	}
	if err != nil {
		log.Error("clean task has err for get ClusterCleanJobKey err: %s", err.Error())
		return
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				bytes := make([]byte, 8)
				binary.LittleEndian.PutUint64(bytes, uint64(time.Now().UnixNano()+scheduleTime))
				if err := service.Store.Put(ctx, entity.ClusterCleanJobKey, bytes); err != nil {
					log.Error("reput master has err:[%s]", err.Error())
					ctx.cancel()
				}
			}
			time.Sleep(time.Duration(scheduleTime / 3))
		}
	}()

	now := time.Now()

	log.Info("Start clean task")

	ctx.nodeMap, ctx.nodeHandlerMap, err = service.QueryNodeHandlerMap(ctx, nil)
	if err != nil {
		log.Error("query all nodes err:[%s]", err)
		//TODO alarm
		return
	}

	if len(ctx.nodeHandlerMap) != len(ctx.nodeMap) {
		log.Error("nodeInfoMap:[%d] not equals nodeMap:[%d] so skip schedule ", len(ctx.nodeHandlerMap), len(ctx.nodeMap))
		return
	}

	if err = collect(ctx, service); err != nil {
		return
	}

	for i := 0; i < len(jobs); i++ {
		jobs[i].process(ctx)
	}

	log.Info("process job over use time:[%v]", time.Now().Sub(now))
	m := entity.Monitor()
	if m != nil {
		m.GetGauge(m.GetCluster(), "schedule", "count").Add(1)
	}
}

func collect(ctx *processContext, service *service.BaseService) (err error) {
	ctx.nodeMap, ctx.nodeHandlerMap, err = service.QueryNodeHandlerMap(ctx, nil)
	if err != nil {
		log.Error("query all nodes err:[%s]", err)
		//TODO alarm
		return
	}

	if len(ctx.nodeHandlerMap) != len(ctx.nodeMap) {
		log.Error("nodeInfoMap:[%d] not equals nodeMap:[%d] so skip schedule ", len(ctx.nodeHandlerMap), len(ctx.nodeMap))
		return
	}

	ctx.rangeMap, err = groupRanges(ctx, service)
	if err != nil {
		log.Error("query all ranges err:[%s]", err)
		//TODO alarm
		return
	}

	//fix ds got range type by db
	for _, nh := range ctx.nodeHandlerMap {
		for _, rh := range nh.RangeHanders {
			if rng := ctx.rangeMap[rh.Id]; rng != nil {
				rh.StoreType = rng.StoreType
			}
		}
	}

	ctx.tableMap, err = groupTables(ctx, service)
	if err != nil {
		log.Error("prefix scan table err:[%s]  ", err.Error())
		//TODO alarm
		return
	}

	ctx.dbMap, err = groupDatabase(ctx, service)
	if err != nil {
		log.Error("prefix scan database err:[%s]  ", err.Error())
		//TODO alarm
		return
	}

	ctx.createRecordMap, err = groupCreateRecords(ctx, service)
	if err != nil {
		log.Error("prefix scan CreateRecord err:[%s]  ", err.Error())
		//TODO alarm
		return
	}
	return
}

func groupDatabase(ctx context.Context, service *service.BaseService) (map[int32]int32, error) {
	bases, err := service.QueryDatabases(ctx)
	databaseMap := make(map[int32]int32, len(bases))
	if err != nil {
		return nil, err
	}

	for _, b := range bases {
		databaseMap[b] = b
	}
	return databaseMap, nil
}

func groupTables(ctx context.Context, service *service.BaseService) (map[int32]*entity.TableRecord, error) {
	group := make(map[int32]*entity.TableRecord)
	_, values, err := service.PrefixScan(ctx, entity.PrefixTable)
	if err != nil {
		return nil, err
	}

	for _, bytes := range values {
		table := &entity.TableRecord{}
		if err := json.Unmarshal(bytes, table); err != nil {
			log.Error("unmarshal table err:[%s] , value:[%s] ", err.Error(), string(bytes))
			continue
		}
		group[table.TableId] = table
	}

	return group, nil
}

//process ranges
func groupRanges(ctx context.Context, service *service.BaseService) (map[uint64]*basepb.Range, error) {

	group := make(map[uint64]*basepb.Range)

	_, values, err := service.PrefixScan(ctx, entity.PrefixRange)
	if err != nil {
		log.Error("prefix scan range err:[%s]  ", err.Error())
		return nil, err
	}

	for _, bytes := range values {
		rgn := &basepb.Range{}
		if err := rgn.Unmarshal(bytes); err != nil {
			log.Error("unmarshal range err:[%s] , value:[%s] ", err.Error(), string(bytes))
			continue
		}
		group[rgn.Id] = rgn
	}

	return group, nil
}

func groupCreateRecords(ctx context.Context, service *service.BaseService) (map[int64]*entity.CreateRecord, error) {
	group := make(map[int64]*entity.CreateRecord)
	_, values, err := service.PrefixScan(ctx, entity.PrefixCreate)
	if err != nil {
		return nil, err
	}

	for _, bytes := range values {
		create := &entity.CreateRecord{}
		if err := json.Unmarshal(bytes, create); err != nil {
			log.Error("unmarshal CreateRecord err:[%s] , value:[%s] ", err.Error(), string(bytes))
			continue
		}
		group[create.CreateId] = create
	}

	return group, nil
}

//the job check range infos and fix it
func StartScheduleJob(ctx context.Context, service *service.BaseService) {

	if entity.Conf().Global.ScheduleSecond <= 0 {
		entity.Conf().Global.ScheduleSecond = 60
		log.Warn("not set ScheduleSecond in config so default:%d", entity.Conf().Global.ScheduleSecond)
	}

	if entity.Conf().Global.PeerDownSecond <= 0 {
		entity.Conf().Global.PeerDownSecond = 300
		log.Warn("not set PeerDownSecond in config so default:%d", entity.Conf().Global.PeerDownSecond)
	}

	if entity.Conf().Global.MemoryRatio <= 0 {
		entity.Conf().Global.MemoryRatio = 0.8
		log.Warn("not set MemoryRation in config so default:%d", entity.Conf().Global.MemoryRatio)
	}

	//diff range and table remove outside range , it need lock table by distlock
	//diff table and node range , to del range in ds
	scheduler := gocron.NewScheduler()
	scheduler.Every(uint64(entity.Conf().Global.ScheduleSecond)).Seconds().Do(process, ctx, service, []ProcessJob{
		&nodeStateJob{service: service},
		&RangeTypeJob{service: service},
		&GCJob{service: service},
		&FailoverJob{service: service},
		&BalanceJob{service: service},
		// add monitor job
		&MonitorJob{service: service},
	})
	<-scheduler.Start()
}

func nodeAddr(node *basepb.Node) string {
	return fmt.Sprintf("%s:%d", node.GetIp(), node.GetServerPort())
}

func printStack() {
	var buf [4096]byte
	n := runtime.Stack(buf[:], false)
	log.Error("==> %s\n", string(buf[:n]))
}
