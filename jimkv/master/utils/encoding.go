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
	"encoding/binary"
	"github.com/jimdb-org/jimdb/jimkv/master/utils/bytes"
	"github.com/jimdb-org/jimdb/jimkv/master/utils/encoding"
)

const (
	//record_data
	Store_Prefix_KV    byte = 1
	Store_Prefix_INDEX byte = 2
)

func DecodeStorePrefix(key []byte) (byte, uint64, uint64) {
	_, v, _ := encoding.DecodeUvarintAscending(key[9:])
	return key[0], bytes.ByteArray2UInt64(key[1:9]), v
}

func EncodeStorePrefix(prefix uint8, id uint64) (start []byte, end []byte) {
	return toByte(prefix, id), toByte(prefix, id+1)
}
func toByte(prefix uint8, id uint64) []byte {
	var buff []byte
	buff = make([]byte, 9)
	buff[0] = prefix
	binary.BigEndian.PutUint64(buff[1:], id)
	return buff
}

//func encodeAscendingKey(buf []byte, col *basepb.Column, sval []byte) ([]byte, error) {
//	switch col.DataType {
//	case basepb.DataType_TinyInt, basepb.DataType_SmallInt, basepb.DataType_Int, basepb.DataType_BigInt:
//		if col.Unsigned {
//			ival, err := strconv.ParseUint(hack.String(sval), 10, 64)
//			if err != nil {
//				return nil, fmt.Errorf("parse unsigned integer failed(%s) when encoding pk(%s) ", err.Error(), col.Name)
//			}
//			return encoding.EncodeUvarintAscending(buf, ival), nil
//		} else {
//			ival, err := strconv.ParseInt(hack.String(sval), 10, 64)
//			if err != nil {
//				return nil, fmt.Errorf("parse integer failed(%s) when encoding pk(%s) ", err.Error(), col.Name)
//			}
//			return encoding.EncodeVarintAscending(buf, ival), nil
//		}
//	case basepb.DataType_Float, basepb.DataType_Double:
//		fval, err := strconv.ParseFloat(hack.String(sval), 64)
//		if err != nil {
//			return nil, fmt.Errorf("parse float failed(%s) when encoding pk(%s)", err.Error(), col.Name)
//		}
//		return encoding.EncodeFloatAscending(buf, fval), nil
//	case basepb.DataType_Varchar, basepb.DataType_Binary, basepb.DataType_Date, basepb.DataType_TimeStamp:
//		return encoding.EncodeBytesAscending(buf, []byte(sval)), nil
//	default:
//		return nil, fmt.Errorf("unsupported type(%s) when encoding pk(%s)", col.DataType.String(), col.Name)
//	}
//}

//func EncodeSplitKeys(keys []string, columns []*basepb.Column) ([][]byte, error) {
//	var ret [][]byte
//	for _, c := range columns {
//		// Code only according to the first primary key
//		if c.GetPrimaryKey() == 1 {
//			for _, k := range keys {
//				buf, err := EncodePrimaryKey(nil, c, []byte(k))
//				if err != nil {
//					return nil, err
//				}
//				ret = append(ret, buf)
//			}
//			break
//		}
//	}
//	return ret, nil
//}

// EncodePrimaryKey encodes the primary key column without encoding the column ID to maintain the sort attribute
//func EncodePrimaryKey(buf []byte, col *basepb.Column, sval []byte) ([]byte, error) {
//	return encodeAscendingKey(buf, col, sval)
//}
