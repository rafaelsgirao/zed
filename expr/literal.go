package expr

import (
	"github.com/brimsec/zq/compiler/ast"
	"github.com/brimsec/zq/zng"
)

type Literal struct {
	zv zng.Value
}

func NewConst(zv zng.Value) (*Literal, error) {
	return &Literal{zv}, nil
}

//XXX only works for primitive... will need zctx for complex literals
func NewLiteral(val ast.Literal) (*Literal, error) {
	zv, err := zng.Parse(val)
	if err != nil {
		return nil, err
	}
	return &Literal{zv}, nil
}

func (l *Literal) Eval(*zng.Record) (zng.Value, error) {
	return l.zv, nil
}
