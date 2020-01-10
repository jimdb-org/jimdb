// Copyright 2016 The kingshard Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package hack

import (
	"reflect"
	"strconv"
	"strings"
	"unsafe"
)

// no copy to change slice to string
// use your own risk
func String(b []byte) (s string) {
	pbytes := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	pstring := (*reflect.StringHeader)(unsafe.Pointer(&s))
	pstring.Data = pbytes.Data
	pstring.Len = pbytes.Len
	return
}

// no copy to change string to slice
// use your own risk
func Slice(s string) (b []byte) {
	pbytes := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	pstring := (*reflect.StringHeader)(unsafe.Pointer(&s))
	pbytes.Data = pstring.Data
	pbytes.Len = pstring.Len
	pbytes.Cap = pstring.Len
	return
}

func IsSqlSep(r rune) bool {
	return r == ' ' || r == ',' ||
		r == '\t' || r == '/' ||
		r == '\n' || r == '\r'
}

func ArrayToString(array []int) string {
	if len(array) == 0 {
		return ""
	}
	var strArray []string
	for _, v := range array {
		strArray = append(strArray, strconv.FormatInt(int64(v), 10))
	}

	return strings.Join(strArray, ", ")
}

//set value to point
func PStr(v string) *string {
	return &v
}

func PInt(v int) *int {
	return &v
}

func PInt8(v int8) *int8 {
	return &v
}

func PInt16(v int16) *int16 {
	return &v
}

func PInt32(v int32) *int32 {
	return &v
}

func PInt64(v int64) *int64 {
	return &v
}

func PFloat32(v float32) *float32 {
	return &v
}

func PFloat64(v float64) *float64 {
	return &v
}

func PRune(v rune) *rune {
	return &v
}

func PBool(v bool) *bool {
	return &v
}

//all point to value
func P2Str(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}

func P2Int(v *int) int {
	if v == nil {
		return 0
	}
	return *v
}

func P2Int8(v *int8) int8 {
	if v == nil {
		return 0
	}
	return *v
}

func P2Int16(v *int16) int16 {
	if v == nil {
		return 0
	}
	return *v
}

func P2Int32(v *int32) int32 {
	if v == nil {
		return 0
	}
	return *v
}

func P2Int64(v *int64) int64 {
	if v == nil {
		return 0
	}
	return *v
}

func P2Float32(v *float32) float32 {
	if v == nil {
		return 0
	}
	return *v
}

func P2Float64(v *float64) float64 {
	if v == nil {
		return 0
	}
	return *v
}

func P2Rune(v *rune) rune {
	if v == nil {
		return 0
	}
	return *v
}

func P2Bool(v *bool) bool {
	if v == nil {
		return false
	}
	return *v
}
