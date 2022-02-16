package agg

import (
	"fmt"

	"github.com/brimdata/zed"
	"github.com/brimdata/zed/pkg/anymath"
)

// A Pattern is a template for creating instances of aggregator functions.
// NewPattern returns a pattern of the type that should be created and
// an instance is created by simply invoking the pattern funtion.
type Pattern func() Function

// MaxValueSize is a limit on an individual aggregation value since sets
// and arrays could otherwise grow without limit leading to a single record
// value that cannot fit in memory.
const MaxValueSize = 20 * 1024 * 1024

type Function interface {
	Consume(*zed.Value)
	ConsumeAsPartial(*zed.Value)
	Result(*zed.Context) *zed.Value
	ResultAsPartial(*zed.Context) *zed.Value
}

func NewPattern(op string) (Pattern, error) {
	switch op {
	case "count":
		return func() Function {
			var c Count
			return &c
		}, nil
	case "any":
		return func() Function {
			return &Any{}
		}, nil
	case "avg":
		return func() Function {
			return &Avg{}
		}, nil
	case "countdistinct":
		return func() Function {
			return NewCountDistinct()
		}, nil
	case "fuse":
		return func() Function {
			return newFuse()
		}, nil
	case "sum":
		return func() Function {
			return newMathReducer(anymath.Add)
		}, nil
	case "min":
		return func() Function {
			return newMathReducer(anymath.Min)
		}, nil
	case "max":
		return func() Function {
			return newMathReducer(anymath.Max)
		}, nil
	case "union":
		return func() Function {
			return newUnion()
		}, nil
	case "collect":
		return func() Function {
			return &Collect{}
		}, nil
	case "and":
		return func() Function {
			return &And{}
		}, nil
	case "or":
		return func() Function {
			return &Or{}
		}, nil
	default:
		return nil, fmt.Errorf("unknown aggregation function: %s", op)
	}
}