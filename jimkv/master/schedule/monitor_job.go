package schedule

import (
	"fmt"

	"github.com/jimdb-org/jimdb/jimkv/master/entity"
	"github.com/jimdb-org/jimdb/jimkv/master/service"
)

var _ ProcessJob = &MonitorJob{}

type MonitorJob struct {
	service *service.BaseService
}

func (mj *MonitorJob) reset(ctx *processContext) {
	if err := collect(ctx, mj.service); err != nil {
		return
	}
	mj.metric(ctx, true)
}

func (mj *MonitorJob) metric(ctx *processContext, reset bool) {
	m := entity.Monitor()
	if m == nil {
		return
	}
	// TODO cluster size
	// database
	if reset {
		m.GetGauge(m.GetCluster(), "database", "count").Set(0)
	} else {
		m.GetGauge(m.GetCluster(), "database", "count").Set(float64(len(ctx.dbMap)))
	}

	tableCount := make(map[int32]int)
	for _, t := range ctx.tableMap {
		tableCount[t.DbId] += 1
	}
	for dbId, count := range tableCount {
		if reset {
			count = 0
		}
		// todo dbId to dbName
		m.GetGauge(m.GetCluster(), "database", "table_count", fmt.Sprint(dbId)).Set(float64(count))
	}

	// table
	// TODO table size
	tableRangeCount := make(map[int32]map[int32]int)
	nodeLeaderCount := make(map[uint64]int)
	tableDbMap := make(map[int32]int32)
	for _, r := range ctx.rangeMap {
		if t, ok := ctx.tableMap[r.TableId]; ok {
			if tableM, ok := tableRangeCount[ctx.dbMap[t.DbId]]; ok {
				tableM[t.TableId] += 1
			} else {
				tableM = make(map[int32]int)
				tableM[t.TableId] += 1
				tableRangeCount[ctx.dbMap[t.DbId]] = tableM
			}
			tableDbMap[t.TableId] = ctx.dbMap[t.DbId]
		}
		if r.Leader > 0 {
			nodeLeaderCount[r.Leader] += 1
		}
	}
	for dbId, tableM := range tableRangeCount {
		for tableId, count := range tableM {
			if reset {
				count = 0
			}
			m.GetGauge(m.GetCluster(), "table", "range_count", fmt.Sprint(dbId), fmt.Sprint(tableId)).Set(float64(count))
		}
	}

	// range
	if reset {
		m.GetGauge(m.GetCluster(), "range", "count").Set(0)
	} else {
		m.GetGauge(m.GetCluster(), "range", "count").Set(float64(len(ctx.rangeMap)))
	}

	// node
	m.GetGauge(m.GetCluster(), "node", "count").Set(float64(len(ctx.nodeMap)))
	for _, node := range ctx.nodeMap {
		var active float64
		if reset {
			active = 0
		} else {
			active = 1
		}
		m.GetGauge(m.GetCluster(), "node", "status",
			fmt.Sprintf("%d", node.Id), fmt.Sprintf("%s:%d", node.Ip, node.ServerPort), node.State.String()).Set(active)
	}
	for nodeId, count := range nodeLeaderCount {
		if node, ok := ctx.nodeMap[nodeId]; ok {
			if reset {
				count = 0
			}
			m.GetGauge(m.GetCluster(), "node", "range_leader_count",
				fmt.Sprintf("%d", node.Id), fmt.Sprintf("%s:%d", node.Ip, node.ServerPort)).Set(float64(count))
		}
	}
	tableReadBytesMap := make(map[string]map[string]uint64)
	tableWriteBytesMap := make(map[string]map[string]uint64)
	tableReadKeysMap := make(map[string]map[string]uint64)
	tableWriteKeysMap := make(map[string]map[string]uint64)
	tableSizeMap := make(map[string]map[string]uint64)
	for _, nodeInfo := range ctx.nodeHandlerMap {
		if reset {
			m.GetGauge(m.GetCluster(), "node", "range_count",
				fmt.Sprintf("%d", nodeInfo.Id), fmt.Sprintf("%s:%d", nodeInfo.Ip, nodeInfo.ServerPort)).Set(0)
		} else {
			m.GetGauge(m.GetCluster(), "node", "range_count",
				fmt.Sprintf("%d", nodeInfo.Id), fmt.Sprintf("%s:%d", nodeInfo.Ip, nodeInfo.ServerPort)).Set(float64(len(nodeInfo.RangeHanders)))
		}

		if nodeInfo.Stats != nil {
			if reset {
				m.GetGauge(m.GetCluster(), "node", "bytes_write",
					fmt.Sprintf("%d", nodeInfo.Id), fmt.Sprintf("%s:%d", nodeInfo.Ip, nodeInfo.ServerPort)).Set(0)
				m.GetGauge(m.GetCluster(), "node", "bytes_read",
					fmt.Sprintf("%d", nodeInfo.Id), fmt.Sprintf("%s:%d", nodeInfo.Ip, nodeInfo.ServerPort)).Set(0)
				m.GetGauge(m.GetCluster(), "node", "capacity",
					fmt.Sprintf("%d", nodeInfo.Id), fmt.Sprintf("%s:%d", nodeInfo.Ip, nodeInfo.ServerPort)).Set(0)
				m.GetGauge(m.GetCluster(), "node", "used",
					fmt.Sprintf("%d", nodeInfo.Id), fmt.Sprintf("%s:%d", nodeInfo.Ip, nodeInfo.ServerPort)).Set(0)
			} else {
				m.GetGauge(m.GetCluster(), "node", "bytes_write",
					fmt.Sprintf("%d", nodeInfo.Id), fmt.Sprintf("%s:%d", nodeInfo.Ip, nodeInfo.ServerPort)).Set(float64(nodeInfo.Stats.WriteBytesPerSec))
				m.GetGauge(m.GetCluster(), "node", "bytes_read",
					fmt.Sprintf("%d", nodeInfo.Id), fmt.Sprintf("%s:%d", nodeInfo.Ip, nodeInfo.ServerPort)).Set(float64(nodeInfo.Stats.ReadBytessPerSec))
				m.GetGauge(m.GetCluster(), "node", "capacity",
					fmt.Sprintf("%d", nodeInfo.Id), fmt.Sprintf("%s:%d", nodeInfo.Ip, nodeInfo.ServerPort)).Set(float64(nodeInfo.Stats.Capacity))
				m.GetGauge(m.GetCluster(), "node", "used",
					fmt.Sprintf("%d", nodeInfo.Id), fmt.Sprintf("%s:%d", nodeInfo.Ip, nodeInfo.ServerPort)).Set(float64(nodeInfo.Stats.UsedSize))
			}

		}
		for _, rangeInfo := range nodeInfo.RangeHanders {
			var dbName, tableName string
			if db, ok := ctx.dbMap[rangeInfo.DbId]; !ok {
				continue
			} else {
				dbName = fmt.Sprint(db)
			}
			if table, ok := ctx.tableMap[rangeInfo.TableId]; !ok {
				continue
			} else {
				tableName = fmt.Sprint(table.TableId)
			}
			if rangeInfo.Status != nil {
				if reset {
					m.GetGauge(m.GetCluster(), "range", "bytes_write",
						dbName, tableName, fmt.Sprintf("%d", rangeInfo.Id)).Set(0)
					m.GetGauge(m.GetCluster(), "range", "bytes_read",
						dbName, tableName, fmt.Sprintf("%d", rangeInfo.Id)).Set(0)
					m.GetGauge(m.GetCluster(), "range", "keys_write",
						dbName, tableName, fmt.Sprintf("%d", rangeInfo.Id)).Set(0)
					m.GetGauge(m.GetCluster(), "range", "keys_read",
						dbName, tableName, fmt.Sprintf("%d", rangeInfo.Id)).Set(0)
				} else {
					m.GetGauge(m.GetCluster(), "range", "bytes_write",
						dbName, tableName, fmt.Sprintf("%d", rangeInfo.Id)).Set(float64(rangeInfo.Status.WriteBytesPerSec))
					m.GetGauge(m.GetCluster(), "range", "bytes_read",
						dbName, tableName, fmt.Sprintf("%d", rangeInfo.Id)).Set(float64(rangeInfo.Status.ReadBytessPerSec))
					m.GetGauge(m.GetCluster(), "range", "keys_write",
						dbName, tableName, fmt.Sprintf("%d", rangeInfo.Id)).Set(float64(rangeInfo.Status.WriteKeysPerSec))
					m.GetGauge(m.GetCluster(), "range", "keys_read",
						dbName, tableName, fmt.Sprintf("%d", rangeInfo.Id)).Set(float64(rangeInfo.Status.ReadKeysPerSec))
				}

				if tableM, ok := tableReadBytesMap[dbName]; ok {
					tableM[tableName] += rangeInfo.Status.ReadBytessPerSec
				} else {
					tableM = make(map[string]uint64)
					tableM[tableName] += rangeInfo.Status.ReadBytessPerSec
					tableReadBytesMap[dbName] = tableM
				}
				if tableM, ok := tableWriteBytesMap[dbName]; ok {
					tableM[tableName] += rangeInfo.Status.WriteBytesPerSec
				} else {
					tableM = make(map[string]uint64)
					tableM[tableName] += rangeInfo.Status.WriteBytesPerSec
					tableWriteBytesMap[dbName] = tableM
				}
				if tableM, ok := tableReadKeysMap[dbName]; ok {
					tableM[tableName] += rangeInfo.Status.ReadKeysPerSec
				} else {
					tableM = make(map[string]uint64)
					tableM[tableName] += rangeInfo.Status.ReadKeysPerSec
					tableReadKeysMap[dbName] = tableM
				}
				if tableM, ok := tableWriteKeysMap[dbName]; ok {
					tableM[tableName] += rangeInfo.Status.WriteKeysPerSec
				} else {
					tableM = make(map[string]uint64)
					tableM[tableName] += rangeInfo.Status.WriteKeysPerSec
					tableWriteKeysMap[dbName] = tableM
				}

				if tableM, ok := tableSizeMap[dbName]; ok {
					tableM[tableName] += rangeInfo.Status.ApproximateSize
				} else {
					tableM = make(map[string]uint64)
					tableM[tableName] += rangeInfo.Status.ApproximateSize
					tableSizeMap[dbName] = tableM
				}
			}
		}
		for dbName, dbM := range tableReadBytesMap {
			for tableName, r := range dbM {
				if reset {
					r = 0
				}
				m.GetGauge(m.GetCluster(), "table", "bytes_read",
					dbName, tableName).Set(float64(r))
			}
		}
		for dbName, dbM := range tableWriteBytesMap {
			for tableName, r := range dbM {
				if reset {
					r = 0
				}
				m.GetGauge(m.GetCluster(), "table", "bytes_write",
					dbName, tableName).Set(float64(r))
			}
		}

		for dbName, dbM := range tableReadKeysMap {
			for tableName, r := range dbM {
				if reset {
					r = 0
				}
				m.GetGauge(m.GetCluster(), "table", "keys_read",
					dbName, tableName).Set(float64(r))
			}
		}
		for dbName, dbM := range tableWriteKeysMap {
			for tableName, r := range dbM {
				if reset {
					r = 0
				}
				m.GetGauge(m.GetCluster(), "table", "keys_write",
					dbName, tableName).Set(float64(r))
			}
		}

		for dbName, dbM := range tableSizeMap {
			for tableName, r := range dbM {
				if reset {
					r = 0
				}
				m.GetGauge(m.GetCluster(), "table", "size",
					dbName, tableName).Set(float64(r))
			}
		}
	}
	if reset {
		m.Push()
	}
}

func (mj *MonitorJob) process(ctx *processContext) {
	mj.metric(ctx, false)
}
