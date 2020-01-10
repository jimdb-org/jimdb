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

package utils

import (
	"bytes"
)

var (
	MaxColumnNameLength = 128
	DefaultRowIDName    = "__row_id"
)

//func ParseTableProperties(properties string) (*entity.TableProperty, error) {
//	tp := new(entity.TableProperty)
//	err := json.Unmarshal([]byte(properties), tp)
//	if err != nil {
//		log.Error("deserialize table property failed, err:[%v]", err)
//		return nil, err
//	}
//	if tp.Columns == nil {
//		log.Error("column is nil")
//		return nil, errs.Error(mspb.ErrorType_InvalidColumn)
//	}
//
//	var index *basepb.Index
//	tp.Columns, index, err = parseColumn(tp.Columns)
//	if err != nil {
//		log.Error("parse table column failed, err:[%v]", err)
//		return nil, err
//	}
//
//	if index != nil {
//		tp.Indexes = append(tp.Indexes, index)
//	}
//
//	if len(tp.Indexes) > 0 {
//		//check index
//		if err = checkIndex(tp); err != nil {
//			return nil, err
//		}
//	}
//
//	for _, col := range tp.Columns {
//		if err = ValidateName(col.Name); err != nil {
//			log.Warn("col[%s] is sql reserved word", col.Name)
//			return nil, err
//		}
//	}
//
//	return tp, nil
//}

//func parseColumn(cols []*basepb.Column) ([]*basepb.Column, *basepb.Index, error) {
//	var hasPk bool
//	var autoCol *basepb.Column
//	var pkIndex int
//
//	colNames := make([]string, 0)
//
//	var index *basepb.Index
//
//	for i, c := range cols {
//		c.Name = strings.ToLower(c.Name)
//		if len(c.GetName()) > MaxColumnNameLength {
//			return nil, nil, errs.Error(mspb.ErrorType_ColumnNameTooLong)
//		}
//		if c.PrimaryKey == 1 {
//			colNames = append(colNames, c.Name)
//			if c.AutoIncrement {
//				if autoCol != nil {
//					return nil, nil, errs.Error(mspb.ErrorType_MissingPk)
//				}
//				autoCol = c
//				pkIndex = i
//			} else {
//				c.PrimaryKey = 0
//			}
//			if c.Nullable {
//				return nil, nil, errs.Error(mspb.ErrorType_PkMustNotNull)
//			}
//			if len(c.DefaultValue) > 0 {
//				return nil, nil, errs.Error(mspb.ErrorType_PkMustNotSetDefaultValue)
//			}
//			hasPk = true
//		}
//		if c.DataType == basepb.DataType_Invalid {
//			return nil, nil, errs.Error(mspb.ErrorType_InvalidColumn)
//		}
//	}
//
//	if !hasPk {
//		return nil, nil, errs.Error(mspb.ErrorType_MissingPk)
//	}
//
//	if autoCol == nil {
//		cols = append([]*basepb.Column{
//			{
//				Name:          DefaultRowIDName,
//				DataType:      basepb.DataType_BigInt,
//				Nullable:      false,
//				PrimaryKey:    1,
//				Index:         true,
//				AutoIncrement: true,
//				Unique:        true,
//				ColType:       basepb.ColumnType_COL_System,
//				Unsigned:      true,
//			},
//		}, cols...)
//
//		index = &basepb.Index{
//			Name:     strings.Join(colNames, "_"),
//			ColNames: colNames,
//			Unique:   true,
//		}
//
//	} else {
//		newCols := append([]*basepb.Column{autoCol}, cols[:pkIndex]...)
//		newCols = append(newCols, cols[pkIndex+1:]...)
//		cols = newCols
//
//		if len(colNames) > 1 {
//			index = &basepb.Index{
//				Name:     strings.Join(colNames, "_"),
//				ColNames: colNames,
//				Unique:   true,
//			}
//		}
//	}
//
//	columnName := make(map[string]interface{})
//	//sort.Sort(ByPrimaryKey(cols))
//	var id uint64 = 1
//	for _, c := range cols {
//
//		// check column name
//		if _, ok := columnName[c.Name]; ok {
//			return nil, nil, errs.Error(mspb.ErrorType_DupColumnName)
//		} else {
//			columnName[c.Name] = nil
//		}
//
//		// set column id
//		c.Id = id
//		id++
//
//	}
//	return cols, index, nil
//}

//func ToTableProperty(cols []*basepb.Column) (string, error) {
//	tp := &entity.TableProperty{
//		Columns: make([]*basepb.Column, 0),
//	}
//	for _, c := range cols {
//		tp.Columns = append(tp.Columns, c)
//	}
//	bs, err := json.Marshal(tp)
//	if err != nil {
//		return "", err
//	} else {
//		return string(bs), nil
//	}
//}
//
//func checkIndex(tp *entity.TableProperty) error {
//	columnMap := make(map[string]*basepb.Column, len(tp.Columns))
//	for _, col := range tp.Columns {
//		columnMap[col.GetName()] = col
//	}
//
//	for _, index := range tp.Indexes {
//		indexCols := index.GetColNames()
//		for _, col := range indexCols {
//			if _, ok := columnMap[col]; !ok {
//				return errs.Error(mspb.ErrorType_InvalidIndex)
//			}
//		}
//
//	}
//	return nil
//}
//
//func ModifyColumn(table *basepb.Table, source []*basepb.Column) error {
//
//	newColMap := make(map[string]uint64, 0)
//
//	oldColNameMap := make(map[string]*basepb.Column)
//	oldColIDMap := make(map[uint64]*basepb.Column)
//
//	var maxID uint64 = 0
//
//	for _, col := range table.Columns {
//		oldColNameMap[col.Name] = col
//		oldColIDMap[col.Id] = col
//		if col.Id > maxID {
//			maxID = col.Id
//		}
//	}
//
//	for _, col := range source {
//		newColMap[col.Name] = col.GetId()
//
//		if col.GetId() == 0 { // add column
//			if col.PrimaryKey == 1 {
//				log.Warn("pk column is not allow change")
//				return errs.Error(mspb.ErrorType_InvalidColumn)
//			}
//
//			if _, find := oldColNameMap[col.Name]; find {
//				log.Warn("column[%s:%s:%s] is already existed", table.GetDbName(), table.GetName(), col.Name)
//				return errs.Error(mspb.ErrorType_DupColumnName)
//			}
//
//			maxID++
//			col.Id = maxID
//			table.Columns = append(table.Columns, col)
//		} else { // column maybe rename
//
//			oldColumn, find := oldColIDMap[col.Id]
//			if !find {
//				log.Warn("column[%s:%s:%s] is not exist", table.GetDbName(), table.GetName(), col.GetId())
//				return errs.Error(mspb.ErrorType_ColumnNotExist)
//			}
//			if oldColumn.Name == col.GetName() {
//				continue
//			}
//
//			oldColumn.Name = col.GetName()
//		}
//	}
//
//	var tartCols []*basepb.Column
//	for _, col := range table.GetColumns() {
//		_, found := newColMap[col.GetName()]
//		if col.PrimaryKey == 1 || found {
//			tartCols = append(tartCols, col)
//		}
//	}
//	table.Columns = tartCols
//
//	props, err := ToTableProperty(table.Columns)
//	if err != nil {
//		return err
//	}
//
//	table.Properties = props
//
//	table.Epoch.ConfVer++
//
//	return nil
//}
//
//func ValidateName(name string) error {
//	rs := []rune(name)
//
//	if sqlReservedWord[strings.ToLower(name)] {
//		return errs.Error(mspb.ErrorType_SqlReservedWord)
//	}
//
//	if len(rs) == 0 {
//		return fmt.Errorf("name can not set empty string")
//	}
//
//	if unicode.IsNumber(rs[0]) {
//		return fmt.Errorf("name : %s can not start with num", name)
//	}
//
//	for _, r := range rs {
//		switch r {
//		case '\t', '\n', '\v', '\f', '\r', ' ', 0x85, 0xA0, '\\', '+', '-', '!', '*', '/', '(', ')', ':', '^', '[', ']', '"', '{', '}', '~', '%', '&', '\'', '<', '>', '?':
//			return fmt.Errorf("name : %s can not has char in name ", `'\t', '\n', '\v', '\f', '\r', ' ', 0x85, 0xA0 , '\\','+', '-', '!', '*', '/', '(', ')', ':' , '^','[',']','"','{','}','~','%','&','\'','<','>','?'`)
//		}
//	}
//	return nil
//}
//
//func MakeRowKeys(tableID, dataRangeNum uint64) (int64, [][]byte) {
//	startPre, endPre := EncodeStorePrefix(Store_Prefix_KV, tableID)
//
//	step := math.MaxInt64 / int64(dataRangeNum)
//
//	keys := make([][]byte, 0, dataRangeNum+1)
//
//	keys = append(keys, startPre)
//
//	var i int64 = 1
//
//	for ; i < int64(dataRangeNum); i++ {
//		keys = append(keys, encoding.EncodeVarintAscending(startPre, step*i))
//	}
//
//	keys = append(keys, endPre)
//
//	return step, keys
//
//}

type ByLetter [][]byte

func (s ByLetter) Len() int           { return len(s) }
func (s ByLetter) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s ByLetter) Less(i, j int) bool { return bytes.Compare(s[i], s[j]) == -1 }

// to validate name for db , table and columns
var sqlReservedWord = map[string]bool{
	"abs":                              true,
	"absolute":                         true,
	"action":                           true,
	"add":                              true,
	"all":                              true,
	"allocate":                         true,
	"alter":                            true,
	"analyse":                          true,
	"analyze":                          true,
	"and":                              true,
	"any":                              true,
	"are":                              true,
	"array":                            true,
	"array_agg":                        true,
	"array_max_cardinality":            true,
	"as":                               true,
	"asc":                              true,
	"asensitive":                       true,
	"assertion":                        true,
	"asymmetric":                       true,
	"at":                               true,
	"atomic":                           true,
	"attributes":                       true,
	"authorization":                    true,
	"avg":                              true,
	"begin":                            true,
	"begin_frame":                      true,
	"begin_partition":                  true,
	"between":                          true,
	"bigint":                           true,
	"binary":                           true,
	"bit":                              true,
	"bit_length":                       true,
	"blob":                             true,
	"boolean":                          true,
	"both":                             true,
	"by":                               true,
	"call":                             true,
	"called":                           true,
	"cardinality":                      true,
	"cascade":                          true,
	"cascaded":                         true,
	"case":                             true,
	"cast":                             true,
	"catalog":                          true,
	"ceil":                             true,
	"ceiling":                          true,
	"char":                             true,
	"character":                        true,
	"character_length":                 true,
	"char_length":                      true,
	"check":                            true,
	"clob":                             true,
	"close":                            true,
	"coalesce":                         true,
	"collate":                          true,
	"collation":                        true,
	"collect":                          true,
	"column":                           true,
	"commit":                           true,
	"condition":                        true,
	"connect":                          true,
	"connection":                       true,
	"constraint":                       true,
	"constraints":                      true,
	"contains":                         true,
	"continue":                         true,
	"convert":                          true,
	"corr":                             true,
	"corresponding":                    true,
	"count":                            true,
	"covar_pop":                        true,
	"covar_samp":                       true,
	"create":                           true,
	"cross":                            true,
	"cube":                             true,
	"cume_dist":                        true,
	"current":                          true,
	"current_catalog":                  true,
	"current_date":                     true,
	"current_default_transform_group":  true,
	"current_path":                     true,
	"current_role":                     true,
	"current_row":                      true,
	"current_schema":                   true,
	"current_time":                     true,
	"current_timestamp":                true,
	"current_transform_group_for_type": true,
	"current_user":                     true,
	"cursor":                           true,
	"cycle":                            true,
	"datalink":                         true,
	"date":                             true,
	"day":                              true,
	"deallocate":                       true,
	"dec":                              true,
	"decimal":                          true,
	"declare":                          true,
	"default":                          true,
	"deferrable":                       true,
	"deferred":                         true,
	"delete":                           true,
	"dense_rank":                       true,
	"deref":                            true,
	"desc":                             true,
	"describe":                         true,
	"descriptor":                       true,
	"deterministic":                    true,
	"diagnostics":                      true,
	"disconnect":                       true,
	"distinct":                         true,
	"dlnewcopy":                        true,
	"dlpreviouscopy":                   true,
	"dlurlcomplete":                    true,
	"dlurlcompleteonly":                true,
	"dlurlcompletewrite":               true,
	"dlurlpath":                        true,
	"dlurlpathonly":                    true,
	"dlurlpathwrite":                   true,
	"dlurlscheme":                      true,
	"dlurlserver":                      true,
	"dlvalue":                          true,
	"do":                               true,
	"domain":                           true,
	"double":                           true,
	"drop":                             true,
	"dynamic":                          true,
	"each":                             true,
	"element":                          true,
	"else":                             true,
	"end":                              true,
	"end-exec":                         true,
	"end_frame":                        true,
	"end_partition":                    true,
	"equals":                           true,
	"escape":                           true,
	"every":                            true,
	"except":                           true,
	"exception":                        true,
	"exec":                             true,
	"execute":                          true,
	"exists":                           true,
	"external":                         true,
	"extract":                          true,
	"false":                            true,
	"fetch":                            true,
	"filter":                           true,
	"first":                            true,
	"first_value":                      true,
	"float":                            true,
	"floor":                            true,
	"for":                              true,
	"foreign":                          true,
	"found":                            true,
	"frame_row":                        true,
	"free":                             true,
	"from":                             true,
	"full":                             true,
	"function":                         true,
	"fusion":                           true,
	"get":                              true,
	"global":                           true,
	"go":                               true,
	"goto":                             true,
	"grant":                            true,
	"group":                            true,
	"grouping":                         true,
	"groups":                           true,
	"having":                           true,
	"hold":                             true,
	"hour":                             true,
	"identity":                         true,
	"immediate":                        true,
	"import":                           true,
	"in":                               true,
	"indicator":                        true,
	"initially":                        true,
	"inner":                            true,
	"inout":                            true,
	"input":                            true,
	"insensitive":                      true,
	"insert":                           true,
	"int":                              true,
	"integer":                          true,
	"intersect":                        true,
	"intersection":                     true,
	"interval":                         true,
	"into":                             true,
	"is":                               true,
	"isolation":                        true,
	"join":                             true,
	"key":                              true,
	"lag":                              true,
	"language":                         true,
	"large":                            true,
	"last":                             true,
	"last_value":                       true,
	"lateral":                          true,
	"lead":                             true,
	"leading":                          true,
	"left":                             true,
	"level":                            true,
	"like":                             true,
	"like_regex":                       true,
	"limit":                            true,
	"ln":                               true,
	"local":                            true,
	"localtime":                        true,
	"localtimestamp":                   true,
	"lock":                             true,
	"lower":                            true,
	"match":                            true,
	"max":                              true,
	"max_cardinality":                  true,
	"member":                           true,
	"merge":                            true,
	"method":                           true,
	"min":                              true,
	"minute":                           true,
	"mod":                              true,
	"modifies":                         true,
	"module":                           true,
	"month":                            true,
	"multiset":                         true,
	"names":                            true,
	"national":                         true,
	"natural":                          true,
	"nchar":                            true,
	"nclob":                            true,
	"new":                              true,
	"next":                             true,
	"no":                               true,
	"none":                             true,
	"normalize":                        true,
	"not":                              true,
	"nth_value":                        true,
	"ntile":                            true,
	"null":                             true,
	"nullif":                           true,
	"numeric":                          true,
	"occurrences_regex":                true,
	"octet_length":                     true,
	"of":                               true,
	"offset":                           true,
	"old":                              true,
	"on":                               true,
	"only":                             true,
	"open":                             true,
	"option":                           true,
	"or":                               true,
	"order":                            true,
	"out":                              true,
	"outer":                            true,
	"output":                           true,
	"over":                             true,
	"overlaps":                         true,
	"overlay":                          true,
	"pad":                              true,
	"parameter":                        true,
	"partial":                          true,
	"partition":                        true,
	"percent":                          true,
	"percentile_cont":                  true,
	"percentile_disc":                  true,
	"percent_rank":                     true,
	"period":                           true,
	"placing":                          true,
	"portion":                          true,
	"position":                         true,
	"position_regex":                   true,
	"power":                            true,
	"precedes":                         true,
	"precision":                        true,
	"prepare":                          true,
	"preserve":                         true,
	"primary":                          true,
	"prior":                            true,
	"privileges":                       true,
	"procedure":                        true,
	"public":                           true,
	"range":                            true,
	"rank":                             true,
	"read":                             true,
	"reads":                            true,
	"real":                             true,
	"recursive":                        true,
	"ref":                              true,
	"references":                       true,
	"referencing":                      true,
	"regr_avgx":                        true,
	"regr_avgy":                        true,
	"regr_count":                       true,
	"regr_intercept":                   true,
	"regr_r2":                          true,
	"regr_slope":                       true,
	"regr_sxx":                         true,
	"regr_sxy":                         true,
	"regr_syy":                         true,
	"relative":                         true,
	"release":                          true,
	"restrict":                         true,
	"result":                           true,
	"return":                           true,
	"returned_cardinality":             true,
	"returning":                        true,
	"returns":                          true,
	"revoke":                           true,
	"right":                            true,
	"rollback":                         true,
	"rollup":                           true,
	"row":                              true,
	"rows":                             true,
	"row_number":                       true,
	"savepoint":                        true,
	"schema":                           true,
	"scope":                            true,
	"scroll":                           true,
	"search":                           true,
	"second":                           true,
	"section":                          true,
	"select":                           true,
	"sensitive":                        true,
	"session":                          true,
	"session_user":                     true,
	"set":                              true,
	"similar":                          true,
	"size":                             true,
	"smallint":                         true,
	"some":                             true,
	"space":                            true,
	"specific":                         true,
	"specifictype":                     true,
	"sql":                              true,
	"sqlcode":                          true,
	"sqlerror":                         true,
	"sqlexception":                     true,
	"sqlstate":                         true,
	"sqlwarning":                       true,
	"sqrt":                             true,
	"start":                            true,
	"static":                           true,
	"stddev_pop":                       true,
	"stddev_samp":                      true,
	"submultiset":                      true,
	"substring":                        true,
	"substring_regex":                  true,
	"succeeds":                         true,
	"sum":                              true,
	"symmetric":                        true,
	"system":                           true,
	"system_time":                      true,
	"system_user":                      true,
	"table":                            true,
	"tablesample":                      true,
	"temporary":                        true,
	"then":                             true,
	"time":                             true,
	"timestamp":                        true,
	"timezone_hour":                    true,
	"timezone_minute":                  true,
	"to":                               true,
	"trailing":                         true,
	"transaction":                      true,
	"translate":                        true,
	"translate_regex":                  true,
	"translation":                      true,
	"treat":                            true,
	"trigger":                          true,
	"trim":                             true,
	"trim_array":                       true,
	"true":                             true,
	"truncate":                         true,
	"uescape":                          true,
	"union":                            true,
	"unique":                           true,
	"unknown":                          true,
	"unnest":                           true,
	"update":                           true,
	"upper":                            true,
	"usage":                            true,
	"user":                             true,
	"using":                            true,
	"value":                            true,
	"values":                           true,
	"value_of":                         true,
	"varbinary":                        true,
	"varchar":                          true,
	"variadic":                         true,
	"varying":                          true,
	"var_pop":                          true,
	"var_samp":                         true,
	"versioning":                       true,
	"view":                             true,
	"when":                             true,
	"whenever":                         true,
	"where":                            true,
	"width_bucket":                     true,
	"window":                           true,
	"with":                             true,
	"within":                           true,
	"without":                          true,
	"work":                             true,
	"write":                            true,
	"xml":                              true,
	"xmlagg":                           true,
	"xmlattributes":                    true,
	"xmlbinary":                        true,
	"xmlcast":                          true,
	"xmlcomment":                       true,
	"xmlconcat":                        true,
	"xmldocument":                      true,
	"xmlelement":                       true,
	"xmlexists":                        true,
	"xmlforest":                        true,
	"xmliterate":                       true,
	"xmlnamespaces":                    true,
	"xmlparse":                         true,
	"xmlpi":                            true,
	"xmlquery":                         true,
	"xmlserialize":                     true,
	"xmltable":                         true,
	"xmltext":                          true,
	"xmlvalidate":                      true,
	"year":                             true,
	"zone":                             true,
}
