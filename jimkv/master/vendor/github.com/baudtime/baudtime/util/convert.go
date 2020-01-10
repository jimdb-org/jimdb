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

package util

import (
	"github.com/baudtime/baudtime/msg"
	backendmsg "github.com/baudtime/baudtime/msg/backend"
	"github.com/prometheus/prometheus/pkg/labels"
)

func MatchersToProto(ms []*labels.Matcher) []*backendmsg.Matcher {
	protoMatchers := make([]*backendmsg.Matcher, 0, len(ms))
	for _, m := range ms {
		protoMatchers = append(protoMatchers, MatcherToProto(m))
	}
	return protoMatchers
}

func MatcherToProto(m *labels.Matcher) *backendmsg.Matcher {
	switch m.Type {
	case labels.MatchEqual:
		return &backendmsg.Matcher{Type: 0, Name: m.Name, Value: m.Value}
	case labels.MatchNotEqual:
		return &backendmsg.Matcher{Type: 1, Name: m.Name, Value: m.Value}
	case labels.MatchRegexp:
		return &backendmsg.Matcher{Type: 2, Name: m.Name, Value: m.Value}
	case labels.MatchNotRegexp:
		return &backendmsg.Matcher{Type: 3, Name: m.Name, Value: m.Value}
	}
	return nil
}

func LabelsToProto(lbs labels.Labels) []msg.Label {
	proto := make([]msg.Label, 0, len(lbs))
	for _, l := range lbs {
		proto = append(proto, msg.Label{Name: l.Name, Value: l.Value})

	}
	return proto
}

func ProtoToLabels(labelPairs []msg.Label) labels.Labels {
	result := make(labels.Labels, 0, len(labelPairs))
	for _, l := range labelPairs {
		result = append(result, labels.Label{
			Name:  l.Name,
			Value: l.Value,
		})
	}
	return result
}
