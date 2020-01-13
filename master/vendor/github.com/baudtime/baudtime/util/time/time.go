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

package time

import (
	"time"

	"github.com/baudtime/baudtime/util/toml"
)

// FromTime returns a new millisecond timestamp from a time.
func FromTime(t time.Time) int64 {
	return t.Unix()*1000 + int64(t.Nanosecond())/int64(time.Millisecond)
}

// Time returns a new time.Time object from a millisecond timestamp.
func Time(ts int64) time.Time {
	return time.Unix(ts/1000, (ts%1000)*int64(time.Millisecond))
}

func DurationMillisec(d time.Duration) int64 {
	return int64(d / (time.Millisecond / time.Nanosecond))
}

func DurationMilliSec(d toml.Duration) int64 {
	return DurationMillisec(time.Duration(d))
}

type TimestampIter struct {
	mint     int64
	maxt     int64
	interval int64
	cur      int64
}

func NewTimestampIter(mint int64, maxt int64, interval int64) *TimestampIter {
	return &TimestampIter{mint, maxt, interval, 0}
}

func (it *TimestampIter) Next() bool {
	if it.cur == 0 {
		it.cur = it.mint
		return true
	}

	if it.interval <= 0 {
		return false
	}

	cur := it.cur + it.interval
	if cur <= it.maxt {
		it.cur = cur
		return true
	}

	return false
}

func (it *TimestampIter) At() int64 {
	return it.cur
}

func (it *TimestampIter) Reset() {
	it.cur = 0
}

func Exponential(d, min, max time.Duration) time.Duration {
	d *= 2
	if d < min {
		d = min
	}
	if d > max {
		d = max
	}
	return d
}
