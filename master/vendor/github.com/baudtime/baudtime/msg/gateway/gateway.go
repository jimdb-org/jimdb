//go:generate msgp -tests=false

package gateway

import (
	"github.com/baudtime/baudtime/msg"
)

type InstantQueryRequest struct {
	Time    string `msg:"time"`
	Timeout string `msg:"timeout"`
	Query   string `msg:"query"`
}

type RangeQueryRequest struct {
	Start   string `msg:"start"`
	End     string `msg:"end"`
	Step    string `msg:"step"`
	Timeout string `msg:"timeout"`
	Query   string `msg:"query"`
}

type QueryResponse struct {
	Result   string         `msg:"result"`
	Status   msg.StatusCode `msg:"status"`
	ErrorMsg string         `msg:"errorMsg"`
}

//msgp:tuple AddRequest
type AddRequest struct {
	Series []*msg.Series `msg:"series"`
}

type LabelValuesRequest struct {
	Name       string `msg:"name"`
	Constraint string `msg:"constraint"`
	Timeout    string `msg:"timeout"`
}
