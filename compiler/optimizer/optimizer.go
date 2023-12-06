package optimizer

import (
	"context"
	"errors"
	"fmt"

	"github.com/brimdata/zed/compiler/ast/dag"
	"github.com/brimdata/zed/compiler/data"
	"github.com/brimdata/zed/lake"
	"github.com/brimdata/zed/order"
	"github.com/brimdata/zed/pkg/field"
	"github.com/segmentio/ksuid"
)

type Optimizer struct {
	ctx  context.Context
	lake *lake.Root
	nent int
}

func New(ctx context.Context, source *data.Source) *Optimizer {
	var lk *lake.Root
	if source != nil {
		lk = source.Lake()
	}
	return &Optimizer{
		ctx:  ctx,
		lake: lk,
	}
}

// mergeFilters transforms the DAG by merging adjacent filter operators so that,
// e.g., "where a | where b" becomes "where a and b".
//
// Note: mergeFilters does not descend into dag.OverExpr.Scope, so it cannot
// merge filters in "over" expressions like "sum(over a | where b | where c)".
func mergeFilters(seq dag.Seq) dag.Seq {
	return walk(seq, true, func(seq dag.Seq) dag.Seq {
		// Start at the next to last element and work toward the first.
		for i := len(seq) - 2; i >= 0; i-- {
			if f1, ok := seq[i].(*dag.Filter); ok {
				if f2, ok := seq[i+1].(*dag.Filter); ok {
					// Merge the second filter into the
					// first and then delete the second.
					f1.Expr = dag.NewBinaryExpr("and", f1.Expr, f2.Expr)
					seq.Delete(i+1, i+2)
				}
			}
		}
		return seq
	})
}

func removePassOps(seq dag.Seq) dag.Seq {
	return walk(seq, true, func(seq dag.Seq) dag.Seq {
		for i := 0; i < len(seq); i++ {
			if _, ok := seq[i].(*dag.Pass); ok {
				seq.Delete(i, i+1)
				i--
				continue
			}
		}
		if len(seq) == 0 {
			seq = dag.Seq{dag.PassOp}
		}
		return seq
	})
}

func walk(seq dag.Seq, over bool, post func(dag.Seq) dag.Seq) dag.Seq {
	for _, op := range seq {
		switch op := op.(type) {
		case *dag.Over:
			if over && op.Body != nil {
				op.Body = walk(op.Body, over, post)
			}
		case *dag.Fork:
			for k := range op.Paths {
				op.Paths[k] = walk(op.Paths[k], over, post)
			}
		case *dag.Scatter:
			for k := range op.Paths {
				op.Paths[k] = walk(op.Paths[k], over, post)
			}
		case *dag.Scope:
			op.Body = walk(op.Body, over, post)
		}
	}
	return post(seq)
}

func walkEntries(seq dag.Seq, post func(dag.Seq) (dag.Seq, error)) (dag.Seq, error) {
	for _, op := range seq {
		switch op := op.(type) {
		case *dag.Fork:
			for k := range op.Paths {
				seq, err := walkEntries(op.Paths[k], post)
				if err != nil {
					return nil, err
				}
				op.Paths[k] = seq
			}
		case *dag.Scatter:
			for k := range op.Paths {
				seq, err := walkEntries(op.Paths[k], post)
				if err != nil {
					return nil, err
				}
				op.Paths[k] = seq
			}
		case *dag.Scope:
			seq, err := walkEntries(op.Body, post)
			if err != nil {
				return nil, err
			}
			op.Body = seq
		}
	}
	return post(seq)
}

// Optimize transforms the DAG by attempting to lift stateless operators
// from the downstream sequence into the trunk of each data source in the From
// operator at the entry point of the DAG.  Once these paths are lifted,
// it also attempts to move any candidate filtering operations into the
// source's pushdown predicate.  This should be called before ParallelizeScan().
// TBD: we need to do pushdown for search/cut to optimize columnar extraction.
func (o *Optimizer) Optimize(seq dag.Seq) (dag.Seq, error) {
	seq = mergeFilters(seq)
	seq = removePassOps(seq)
	o.optimizeParallels(seq)
	seq = mergeFilters(seq)
	seq, err := o.optimizeSourcePaths(seq)
	if err != nil {
		return nil, err
	}
	seq = insertDemand(seq)
	seq = removePassOps(seq)
	return seq, nil
}

func (o *Optimizer) OptimizeDeleter(seq dag.Seq, replicas int) (dag.Seq, error) {
	if len(seq) != 2 {
		return nil, errors.New("internal error: bad deleter structure")
	}
	scan, ok := seq[0].(*dag.DeleteScan)
	if !ok {
		return nil, errors.New("internal error: bad deleter structure")
	}
	filter, ok := seq[1].(*dag.Filter)
	if !ok {
		return nil, errors.New("internal error: bad deleter structure")
	}
	lister := &dag.Lister{
		Kind:   "Lister",
		Pool:   scan.ID,
		Commit: scan.Commit,
	}
	sortKey, err := o.sortKeyOfSource(lister)
	if err != nil {
		return nil, err
	}
	deleter := &dag.Deleter{
		Kind:  "Deleter",
		Pool:  scan.ID,
		Where: filter.Expr,
		//XXX KeyPruner?
	}
	lister.KeyPruner = maybeNewRangePruner(filter.Expr, sortKey)
	scatter := &dag.Scatter{Kind: "Scatter"}
	for k := 0; k < replicas; k++ {
		scatter.Paths = append(scatter.Paths, copyOps(dag.Seq{deleter}))
	}
	var merge dag.Op
	if sortKey.IsNil() {
		merge = &dag.Combine{Kind: "Combine"}
	} else {
		merge = &dag.Merge{
			Kind:  "Merge",
			Expr:  &dag.This{Kind: "This", Path: sortKey.Primary()},
			Order: sortKey.Order,
		}
	}
	return dag.Seq{lister, scatter, merge}, nil
}

func (o *Optimizer) optimizeSourcePaths(seq dag.Seq) (dag.Seq, error) {
	return walkEntries(seq, func(seq dag.Seq) (dag.Seq, error) {
		if len(seq) == 0 {
			return nil, errors.New("internal error: optimizer encountered empty sequential operator")
		}
		o.nent++
		chain := seq[1:]
		if len(chain) == 0 {
			// Nothing to push down.
			return seq, nil
		}
		o.propagateSortKey(seq, []order.SortKey{order.Nil})
		// See if we can lift a filtering predicate into the source op.
		// Filter might be nil in which case we just put the chain back
		// on the source op and zero out the source's filter.
		filter, chain := matchFilter(chain)
		switch op := seq[0].(type) {
		case *dag.PoolScan:
			// Here we transform a PoolScan into a Lister followed by one or more chains
			// of slicers and sequence scanners.  We'll eventually choose other configurations
			// here based on metadata and availability of VNG.
			lister := &dag.Lister{
				Kind:   "Lister",
				Pool:   op.ID,
				Commit: op.Commit,
			}
			// Check to see if we can add a range pruner when the pool key is used
			// in a normal filtering operation.
			sortKey, err := o.sortKeyOfSource(op)
			if err != nil {
				return nil, err
			}
			lister.KeyPruner = maybeNewRangePruner(filter, sortKey)
			seq = dag.Seq{lister}
			_, _, orderRequired, _, err := o.concurrentPath(chain, sortKey)
			if err != nil {
				return nil, err
			}
			if orderRequired {
				seq = append(seq, &dag.Slicer{Kind: "Slicer"})
			}
			seq = append(seq, &dag.SeqScan{
				Kind:      "SeqScan",
				Pool:      op.ID,
				Filter:    filter,
				KeyPruner: lister.KeyPruner,
			})
			seq = append(seq, chain...)
		case *dag.FileScan:
			op.Filter = filter
			seq = append(dag.Seq{op}, chain...)
		case *dag.CommitMetaScan:
			if op.Tap {
				sortKey, err := o.sortKeyOfSource(op)
				if err != nil {
					return nil, err
				}
				// Check to see if we can add a range pruner when the pool key is used
				// in a normal filtering operation.
				op.KeyPruner = maybeNewRangePruner(filter, sortKey)
				// Delete the downstream operators when we are tapping the object list.
				seq = dag.Seq{op}
			}
		case *dag.DefaultScan:
			op.Filter = filter
			seq = append(dag.Seq{op}, chain...)
		}
		return seq, nil
	})
}

// propagateSortKey analyzes a Seq and attempts to push the scan order of the data source
// into the first downstream aggregation.  (We could continue the analysis past that
// point but don't bother yet because we do not yet support any optimization
// past the first aggregation.)  For parallel paths, we propagate
// the scan order if its the same at egress of all of the paths.
func (o *Optimizer) propagateSortKey(seq dag.Seq, parents []order.SortKey) ([]order.SortKey, error) {
	if len(seq) == 0 {
		return parents, nil
	}
	for _, op := range seq {
		var err error
		parents, err = o.propagateSortKeyOp(op, parents)
		if err != nil {
			return []order.SortKey{order.Nil}, err
		}
	}
	return parents, nil
}

func (o *Optimizer) propagateSortKeyOp(op dag.Op, parents []order.SortKey) ([]order.SortKey, error) {
	if join, ok := op.(*dag.Join); ok {
		if len(parents) != 2 {
			return nil, errors.New("internal error: join does not have two parents")
		}
		if fieldOf(join.LeftKey).Equal(parents[0].Primary()) {
			join.LeftDir = parents[0].Order.Direction()
		}
		if fieldOf(join.RightKey).Equal(parents[1].Primary()) {
			join.RightDir = parents[1].Order.Direction()
		}
		// XXX There is definitely a way to propagate the sort key but there's
		// some complexity here. The propagated sort key should be whatever key
		// remains between the left and right join keys. If both the left and
		// right key are part of the resultant record what should the
		// propagated sort key be? Ideally in this case they both would which
		// would allow for maximum flexibility. For now just return unspecified
		// sort order.
		return []order.SortKey{order.Nil}, nil
	}
	// If the op is not a join then condense sort order into a single parent,
	// since all the ops only care about the sort order of multiple parents if
	// the SortKey of all parents is unified.
	parent := order.Nil
	for k, p := range parents {
		if k == 0 {
			parent = p
		} else if !parent.Equal(p) {
			parent = order.Nil
			break
		}
	}
	switch op := op.(type) {
	case *dag.Summarize:
		//XXX handle only primary key for now
		key := parent.Primary()
		for _, k := range op.Keys {
			if groupByKey := fieldOf(k.LHS); groupByKey.Equal(key) {
				rhsExpr := k.RHS
				rhs := fieldOf(rhsExpr)
				if rhs.Equal(key) || orderPreservingCall(rhsExpr, groupByKey) {
					op.InputSortDir = orderAsDirection(parent.Order)
					// Currently, the groupby operator will sort its
					// output according to the primary key, but we
					// should relax this and do an analysis here as
					// to whether the sort is necessary for the
					// downstream consumer.
					return []order.SortKey{parent}, nil
				}
			}
		}
		// We'll live this as unknown for now even though the groupby
		// and not try to optimize downstream of the first groupby
		// unless there is an excplicit sort encountered.
		return nil, nil
	case *dag.Fork:
		var keys []order.SortKey
		for _, seq := range op.Paths {
			out, err := o.propagateSortKey(seq, []order.SortKey{parent})
			if err != nil {
				return nil, err
			}
			keys = append(keys, out...)
		}
		return keys, nil
	case *dag.Scatter:
		var keys []order.SortKey
		for _, seq := range op.Paths {
			out, err := o.propagateSortKey(seq, []order.SortKey{parent})
			if err != nil {
				return nil, err
			}
			keys = append(keys, out...)
		}
		return keys, nil
	case *dag.Merge:
		sortKey := order.NewSortKey(op.Order, nil)
		if this, ok := op.Expr.(*dag.This); ok {
			sortKey.Keys = field.List{this.Path}
		}
		if !sortKey.Equal(parent) {
			sortKey = order.Nil
		}
		return []order.SortKey{sortKey}, nil
	case *dag.PoolScan, *dag.Lister, *dag.SeqScan, *dag.DefaultScan:
		out, err := o.sortKeyOfSource(op)
		return []order.SortKey{out}, err
	default:
		out, err := o.analyzeSortKey(op, parent)
		return []order.SortKey{out}, err
	}
}

func (o *Optimizer) sortKeyOfSource(op dag.Op) (order.SortKey, error) {
	switch op := op.(type) {
	case *dag.DefaultScan:
		return op.SortKey, nil
	case *dag.FileScan:
		return op.SortKey, nil
	case *dag.HTTPScan:
		return op.SortKey, nil
	case *dag.PoolScan:
		return o.sortKey(op.ID)
	case *dag.Lister:
		return o.sortKey(op.Pool)
	case *dag.SeqScan:
		return o.sortKey(op.Pool)
	case *dag.CommitMetaScan:
		if op.Tap && op.Meta == "objects" {
			// For a tap into the object stream, we compile the downstream
			// DAG as if it were a normal query (so the optimizer can prune
			// objects etc.) but we execute it in the end as a meta-query.
			return o.sortKey(op.Pool)
		}
		return order.Nil, nil //XXX is this right?
	default:
		return order.Nil, fmt.Errorf("internal error: unknown source type %T", op)
	}
}

func (o *Optimizer) sortKey(id ksuid.KSUID) (order.SortKey, error) {
	pool, err := o.lookupPool(id)
	if err != nil {
		return order.Nil, err
	}
	return pool.SortKey, nil
}

// Parallelize tries to parallelize the DAG by splitting each source
// path as much as possible of the sequence into n parallel branches.
func (o *Optimizer) Parallelize(seq dag.Seq, n int) (dag.Seq, error) {
	// Compute the number of parallel paths across all input sources to
	// achieve the desired level of concurrency.  At some point, we should
	// use a semaphore here and let each possible path use the max concurrency.
	if o.nent == 0 {
		return seq, nil
	}
	concurrency := n / o.nent
	if concurrency < 2 {
		concurrency = 2
	}
	seq, err := walkEntries(seq, func(seq dag.Seq) (dag.Seq, error) {
		if len(seq) == 0 {
			return seq, nil
		}
		var front dag.Seq
		var tail []dag.Op
		if lister, slicer, rest := matchSource(seq); lister != nil {
			// We parallelize the scanning to achieve the desired concurrency,
			// then the step below pulls downstream operators into the parallel
			// branches when possible, e.g., to parallelize aggregations etc.
			front.Append(lister)
			if slicer != nil {
				front.Append(slicer)
			}
			tail = rest
		} else if scan, ok := seq[0].(*dag.DefaultScan); ok {
			front.Append(scan)
			tail = seq[1:]
		} else {
			return seq, nil
		}
		if len(tail) == 0 {
			return seq, nil
		}
		parallel, err := o.parallelizeScan(tail, concurrency)
		if err != nil {
			return nil, err
		}
		if parallel == nil {
			// Leave the source path unmodified.
			return seq, nil
		}
		// Replace the source path with the parallelized gadget.
		return append(front, parallel...), nil
	})
	if err != nil {
		return nil, err
	}
	o.optimizeParallels(seq)
	return removePassOps(seq), nil
}

func (o *Optimizer) lookupPool(id ksuid.KSUID) (*lake.Pool, error) {
	if o.lake == nil {
		return nil, errors.New("internal error: lake operation cannot be used in non-lake context")
	}
	// This is fast because of the pool cache in the lake.
	return o.lake.OpenPool(o.ctx, id)
}

func matchSource(ops []dag.Op) (*dag.Lister, *dag.Slicer, []dag.Op) {
	lister, ok := ops[0].(*dag.Lister)
	if !ok {
		return nil, nil, nil
	}
	ops = ops[1:]
	slicer, ok := ops[0].(*dag.Slicer)
	if ok {
		ops = ops[1:]
	}
	if _, ok := ops[0].(*dag.SeqScan); !ok {
		panic("parseSource: no SeqScan")
	}
	return lister, slicer, ops
}

// matchFilter attempts to find a filter from the front of op chain
// and returns the filter's expression (and the modified chain) so
// we can lift the filter predicate into the scanner.
func matchFilter(in []dag.Op) (dag.Expr, []dag.Op) {
	if len(in) == 0 {
		return nil, in
	}
	filter, ok := in[0].(*dag.Filter)
	if !ok {
		return nil, in
	}
	return filter.Expr, in[1:]
}

// newRangePruner returns a new predicate based on the input predicate pred
// that when applied to an input value (i.e., "this") with fields from/to, returns
// true if comparisons in pred against literal values can for certain rule out
// that pred would be true for any value in the from/to range.  From/to are presumed
// to be ordered according to the order o.  This is used to prune metadata objects
// from a scan when we know the pool key range of the object could not satisfy
// the filter predicate of any of the values in the object.
func newRangePruner(pred dag.Expr, fld field.Path, o order.Which) dag.Expr {
	min := &dag.This{Kind: "This", Path: field.Path{"min"}}
	max := &dag.This{Kind: "This", Path: field.Path{"max"}}
	if e := buildRangePruner(pred, fld, min, max); e != nil {
		return e
	}
	return nil
}

func maybeNewRangePruner(pred dag.Expr, sortKey order.SortKey) dag.Expr {
	if !sortKey.IsNil() && pred != nil {
		return newRangePruner(pred, sortKey.Primary(), sortKey.Order)
	}
	return nil
}

// buildRangePruner creates a DAG comparison expression that can evalaute whether
// a Zed value adhering to the from/to pattern can be excluded from a scan because
// the expression pred would evaluate to false for all values of fld in the
// from/to value range.  If a pruning decision cannot be reliably determined then
// the return value is nil.
func buildRangePruner(pred dag.Expr, fld field.Path, min, max *dag.This) *dag.BinaryExpr {
	e, ok := pred.(*dag.BinaryExpr)
	if !ok {
		// If this isn't a binary predicate composed of comparison operators, we
		// simply punt here.  This doesn't mean we can't optimize, because if the
		// unknown part (from here) appears in the context of an "and", then we
		// can still prune the known side of the "and" as implemented in the
		// logic below.
		return nil
	}
	switch e.Op {
	case "and":
		// For an "and", if we know either side is prunable, then we can prune
		// because both conditions are required.  So we "or" together the result
		// when both sub-expressions are valid.
		lhs := buildRangePruner(e.LHS, fld, min, max)
		rhs := buildRangePruner(e.RHS, fld, min, max)
		if lhs == nil {
			return rhs
		}
		if rhs == nil {
			return lhs
		}
		return dag.NewBinaryExpr("or", lhs, rhs)
	case "or":
		// For an "or", if we know both sides are prunable, then we can prune
		// because either condition is required.  So we "and" together the result
		// when both sub-expressions are valid.
		lhs := buildRangePruner(e.LHS, fld, min, max)
		rhs := buildRangePruner(e.RHS, fld, min, max)
		if lhs == nil || rhs == nil {
			return nil
		}
		return dag.NewBinaryExpr("and", lhs, rhs)
	case "==", "<", "<=", ">", ">=":
		this, literal, op := literalComparison(e)
		if this == nil || !fld.Equal(this.Path) {
			return nil
		}
		// At this point, we know we can definitely run a pruning decision based
		// on the literal value we found, the comparison op, and the lower/upper bounds.
		return rangePrunerPred(op, literal, min, max)
	default:
		return nil
	}
}

func rangePrunerPred(op string, literal *dag.Literal, min, max *dag.This) *dag.BinaryExpr {
	switch op {
	case "<":
		// key < CONST
		return compare("<=", literal, min)
	case "<=":
		// key <= CONST
		return compare("<", literal, min)
	case ">":
		// key > CONST
		return compare(">=", literal, max)
	case ">=":
		// key >= CONST
		return compare(">", literal, max)
	case "==":
		// key == CONST
		return dag.NewBinaryExpr("or",
			compare(">", min, literal),
			compare("<", max, literal))
	}
	panic("rangePrunerPred unknown op " + op)
}

// compare returns a DAG expression for a standard comparison operator but
// uses a call to the Zed language function "compare()" as standard comparisons
// do not handle nullsmax or cross-type comparisons (which can arise when the
// pool key value type changes).
func compare(op string, lhs, rhs dag.Expr) *dag.BinaryExpr {
	nullsMax := &dag.Literal{Kind: "Literal", Value: "true"}
	call := &dag.Call{Kind: "Call", Name: "compare", Args: []dag.Expr{lhs, rhs, nullsMax}}
	return dag.NewBinaryExpr(op, call, &dag.Literal{Kind: "Literal", Value: "0"})
}

func literalComparison(e *dag.BinaryExpr) (*dag.This, *dag.Literal, string) {
	switch lhs := e.LHS.(type) {
	case *dag.This:
		if rhs, ok := e.RHS.(*dag.Literal); ok {
			return lhs, rhs, e.Op
		}
	case *dag.Literal:
		if rhs, ok := e.RHS.(*dag.This); ok {
			return rhs, lhs, reverseComparator(e.Op)
		}
	}
	return nil, nil, ""
}

func reverseComparator(op string) string {
	switch op {
	case "==", "!=":
		return op
	case "<":
		return ">="
	case "<=":
		return ">"
	case ">":
		return "<="
	case ">=":
		return "<"
	}
	panic("unknown op")
}
