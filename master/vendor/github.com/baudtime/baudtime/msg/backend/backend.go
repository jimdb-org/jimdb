//go:generate msgp -tests=false

package backend

import (
	"github.com/baudtime/baudtime/msg"
)

type MatchType byte

const (
	MatchType_MatchEqual MatchType = iota
	MatchType_MatchNotEqual
	MatchType_MatchRegexp
	MatchType_MatchNotRegexp
)

func (z MatchType) String() string {
	switch z {
	case MatchType_MatchEqual:
		return "Equal"
	case MatchType_MatchNotEqual:
		return "NotEqual"
	case MatchType_MatchRegexp:
		return "Regexp"
	case MatchType_MatchNotRegexp:
		return "NotRegexp"
	}
	return "<Invalid>"
}

type Matcher struct {
	Type  MatchType `msg:"Type"`
	Name  string    `msg:"Name"`
	Value string    `msg:"Value"`
}

type SelectRequest struct {
	Mint     int64      `msg:"mint"`
	Maxt     int64      `msg:"maxt"`
	Interval int64      `msg:"interval"`
	Matchers []*Matcher `msg:"matchers"`
	SpanCtx  []byte     `msg:"spanCtx"`
}

type SelectResponse struct {
	Status   msg.StatusCode `msg:"status"`
	Series   []*msg.Series  `msg:"series"`
	ErrorMsg string         `msg:"errorMsg"`
}

//msgp:tuple AddRequest
type AddRequest struct {
	Series []*msg.Series `msg:"series"`
}

type LabelValuesRequest struct {
	Mint     int64      `msg:"mint"`
	Maxt     int64      `msg:"maxt"`
	Name     string     `msg:"name"`
	Matchers []*Matcher `msg:"matchers"`
	SpanCtx  []byte     `msg:"spanCtx"`
}
