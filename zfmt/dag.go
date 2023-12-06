package zfmt

import (
	"github.com/brimdata/zed/compiler/ast/dag"
	astzed "github.com/brimdata/zed/compiler/ast/zed"
	"github.com/brimdata/zed/order"
	"github.com/brimdata/zed/zson"
)

func DAG(seq dag.Seq) string {
	d := &canonDAG{
		canonZed: canonZed{formatter: formatter{tab: 2}},
		head:     true,
		first:    true,
	}
	d.seq(seq)
	d.flush()
	return d.String()
}

func DAGExpr(e dag.Expr) string {
	d := &canonDAG{
		canonZed: canonZed{formatter: formatter{tab: 2}},
		head:     true,
		first:    true,
	}
	d.expr(e, "")
	d.flush()
	return d.String()
}

type canonDAG struct {
	canonZed
	head  bool
	first bool
}

func (c *canonDAG) open(args ...interface{}) {
	c.formatter.open(args...)
}

func (c *canonDAG) close() {
	c.formatter.close()
}

func (c *canonDAG) assignments(assignments []dag.Assignment) {
	for k, a := range assignments {
		if k > 0 {
			c.write(",")
		}
		if a.LHS != nil {
			c.expr(a.LHS, "")
			c.write(":=")
		}
		c.expr(a.RHS, "")
	}
}

func (c *canonDAG) exprs(exprs []dag.Expr) {
	for k, e := range exprs {
		if k > 0 {
			c.write(", ")
		}
		c.expr(e, "")
	}
}

func (c *canonDAG) expr(e dag.Expr, parent string) {
	switch e := e.(type) {
	case nil:
		c.write("null")
	case *dag.Agg:
		c.write("%s(", e.Name)
		if e.Expr != nil {
			c.expr(e.Expr, "")
		}
		c.write(")")
		if e.Where != nil {
			c.write(" where ")
			c.expr(e.Where, "")
		}
	case *astzed.Primitive:
		c.literal(*e)
	case *dag.UnaryExpr:
		c.write(e.Op)
		c.expr(e.Operand, "not")
	case *dag.BinaryExpr:
		c.binary(e, parent)
	case *dag.Conditional:
		c.write("(")
		c.expr(e.Cond, "")
		c.write(") ? ")
		c.expr(e.Then, "")
		c.write(" : ")
		c.expr(e.Else, "")
	case *dag.Call:
		c.write("%s(", e.Name)
		c.exprs(e.Args)
		c.write(")")
	case *dag.OverExpr:
		c.open("(")
		c.ret()
		c.write("over ")
		c.exprs(e.Exprs)
		if len(e.Defs) > 0 {
			for i, d := range e.Defs {
				if i > 0 {
					c.write(", ")
				}
				c.write("%s=", d.Name)
				c.expr(d.Expr, "")
			}
		}
		c.seq(e.Body)
		c.close()
		c.ret()
		c.flush()
		c.write(")")
	case *dag.Search:
		c.write("search(%s)", e.Value)
	case *dag.This:
		c.fieldpath(e.Path)
	case *dag.Var:
		c.write("%s", e.Name)
	case *dag.Literal:
		c.write("%s", e.Value)
	case *astzed.TypeValue:
		c.write("type<")
		c.typ(e.Value)
		c.write(">")
	case *dag.RecordExpr:
		c.write("{")
		for k, elem := range e.Elems {
			if k > 0 {
				c.write(",")
			}
			switch e := elem.(type) {
			case *dag.Field:
				c.write(zson.QuotedName(e.Name))
				c.write(":")
				c.expr(e.Value, "")
			case *dag.Spread:
				c.write("...")
				c.expr(e.Expr, "")
			default:
				c.write("zfmt: unknown record elem type: %T", e)
			}
		}
		c.write("}")
	case *dag.ArrayExpr:
		c.write("[")
		c.vectorElems(e.Elems)
		c.write("]")
	case *dag.SetExpr:
		c.write("|[")
		c.vectorElems(e.Elems)
		c.write("]|")
	case *dag.MapExpr:
		c.write("|{")
		for k, e := range e.Entries {
			if k > 0 {
				c.write(",")
			}
			c.expr(e.Key, "")
			c.write(":")
			c.expr(e.Value, "")
		}
		c.write("}|")
	default:
		c.open("(unknown expr %T)", e)
		c.close()
		c.ret()
	}
}

func (c *canonDAG) binary(e *dag.BinaryExpr, parent string) {
	switch e.Op {
	case ".":
		if !isDAGThis(e.LHS) {
			c.expr(e.LHS, "")
			c.write(".")
		}
		c.expr(e.RHS, "")
	case "[":
		if isDAGThis(e.LHS) {
			c.write(".")
		} else {
			c.expr(e.LHS, "")
		}
		c.write("[")
		c.expr(e.RHS, "")
		c.write("]")
	case "in", "and", "or":
		parens := needsparens(parent, e.Op)
		c.maybewrite("(", parens)
		c.expr(e.LHS, e.Op)
		c.write(" %s ", e.Op)
		c.expr(e.RHS, e.Op)
		c.maybewrite(")", parens)
	default:
		parens := needsparens(parent, e.Op)
		c.maybewrite("(", parens)
		c.expr(e.LHS, e.Op)
		c.write("%s", e.Op)
		c.expr(e.RHS, e.Op)
		c.maybewrite(")", parens)
	}
}

func (c *canonDAG) vectorElems(elems []dag.VectorElem) {
	for k, elem := range elems {
		if k > 0 {
			c.write(",")
		}
		switch elem := elem.(type) {
		case *dag.Spread:
			c.write("...")
			c.expr(elem.Expr, "")
		case *dag.VectorValue:
			c.expr(elem.Expr, "")
		default:
			c.write("zfmt: unknown vector elem type: %T", elem)
		}
	}
}

func isDAGThis(e dag.Expr) bool {
	if this, ok := e.(*dag.This); ok {
		if len(this.Path) == 0 {
			return true
		}
	}
	return false
}

func (c *canonDAG) maybewrite(s string, do bool) {
	if do {
		c.write(s)
	}
}

func (c *canonDAG) next() {
	if c.first {
		c.first = false
	} else {
		c.write("\n")
	}
	c.needRet = false
	c.writeTab()
	if c.head {
		c.head = false
	} else {
		c.write("| ")
	}
}

func (c *canonDAG) seq(seq dag.Seq) {
	for _, p := range seq {
		c.op(p)
	}
}

func (c *canonDAG) op(p dag.Op) {
	switch p := p.(type) {
	case *dag.Scope:
		c.next()
		c.scope(p)
	case *dag.Fork:
		c.next()
		c.open("fork (")
		for _, seq := range p.Paths {
			c.ret()
			c.write("=>")
			c.open()
			c.head = true
			c.seq(seq)
			c.close()
		}
		c.close()
		c.ret()
		c.flush()
		c.write(")")
	case *dag.Scatter:
		c.next()
		c.open("scatter (")
		for _, seq := range p.Paths {
			c.ret()
			c.write("=>")
			c.open()
			c.head = true
			c.seq(seq)
			c.close()
		}
		c.close()
		c.ret()
		c.flush()
		c.write(")")
	case *dag.Switch:
		c.next()
		c.open("switch ")
		if p.Expr != nil {
			c.expr(p.Expr, "")
			c.write(" ")
		}
		c.open("(")
		for _, k := range p.Cases {
			c.ret()
			if k.Expr != nil {
				c.write("case ")
				c.expr(k.Expr, "")
			} else {
				c.write("default")
			}
			c.write(" =>")
			c.open()
			c.head = true
			c.seq(k.Path)
			c.close()
		}
		c.close()
		c.ret()
		c.flush()
		c.write(")")
	case *dag.Merge:
		c.next()
		c.write("merge ")
		c.expr(p.Expr, "")
		c.write(":" + p.Order.String())
	case *dag.Summarize:
		c.next()
		c.open("summarize")
		if p.PartialsIn {
			c.write(" partials-in")
		}
		if p.PartialsOut {
			c.write(" partials-out")
		}
		if p.InputSortDir != 0 {
			c.write(" sort-dir %d", p.InputSortDir)
		}
		c.ret()
		c.open()
		c.assignments(p.Aggs)
		if len(p.Keys) != 0 {
			c.write(" by ")
			c.assignments(p.Keys)
		}
		if p.Limit != 0 {
			c.write(" -with limit %d", p.Limit)
		}
		c.close()
		c.close()
	case *dag.Combine:
		c.next()
		c.write("combine")
	case *dag.Cut:
		c.next()
		c.write("cut ")
		c.assignments(p.Args)
	case *dag.Drop:
		c.next()
		c.write("drop ")
		c.exprs(p.Args)
	case *dag.Sort:
		c.next()
		c.write("sort")
		if p.Order == order.Desc {
			c.write(" -r")
		}
		if p.NullsFirst {
			c.write(" -nulls first")
		}
		if len(p.Args) > 0 {
			c.space()
			c.exprs(p.Args)
		}
	case *dag.Load:
		c.next()
		c.write("load %s", p.Pool)
		if p.Branch != "" {
			c.write("@%s", p.Branch)
		}
		if p.Author != "" {
			c.write(" author %s", p.Author)
		}
		if p.Message != "" {
			c.write(" message %s", p.Message)
		}
		if p.Meta != "" {
			c.write(" meta %s", p.Meta)
		}
	case *dag.Head:
		c.next()
		c.write("head %d", p.Count)
	case *dag.Tail:
		c.next()
		c.write("tail %d", p.Count)
	case *dag.Uniq:
		c.next()
		c.write("uniq")
		if p.Cflag {
			c.write(" -c")
		}
	case *dag.Filter:
		c.next()
		c.open("where ")
		if isDAGTrue(p.Expr) {
			c.write("*")
		} else {
			c.expr(p.Expr, "")
		}
		c.close()
	case *dag.Top:
		c.next()
		c.write("top limit=%d flush=%t ", p.Limit, p.Flush)
		c.exprs(p.Args)
	case *dag.Put:
		c.next()
		c.write("put ")
		c.assignments(p.Args)
	case *dag.Rename:
		c.next()
		c.write("rename ")
		c.assignments(p.Args)
	case *dag.Fuse:
		c.next()
		c.write("fuse")
	case *dag.Join:
		c.next()
		c.open("join on ")
		c.expr(p.LeftKey, "")
		c.write("=")
		c.expr(p.RightKey, "")
		if len(p.Args) != 0 {
			c.write(" ")
			c.assignments(p.Args)
		}
		c.close()
	case *dag.Lister:
		c.next()
		c.open("lister")
		c.write(" pool %s commit %s", p.Pool, p.Commit)
		if p.KeyPruner != nil {
			c.write(" pruner (")
			c.expr(p.KeyPruner, "")
			c.write(")")
		}
		c.close()
	case *dag.SeqScan:
		c.next()
		c.open("seqscan")
		c.write(" pool %s", p.Pool)
		if p.KeyPruner != nil {
			c.write(" pruner (")
			c.expr(p.KeyPruner, "")
			c.write(")")
		}
		if p.Filter != nil {
			c.write(" filter (")
			c.expr(p.Filter, "")
			c.write(")")
		}
		c.close()
	case *dag.Slicer:
		c.next()
		c.open("slicer")
		c.close()
	case *dag.Over:
		c.over(p)
	case *dag.Yield:
		c.next()
		c.write("yield ")
		c.exprs(p.Exprs)
	case *dag.DefaultScan:
		c.next()
		c.write("reader")
		if p.Filter != nil {
			c.write(" filter (")
			c.expr(p.Filter, "")
			c.write(")")
		}
	case *dag.FileScan:
		c.next()
		c.write("file %s", p.Path)
		if p.Format != "" {
			c.write(" format %s", p.Format)
		}
		if !p.SortKey.IsNil() {
			c.write(" order %s", p.SortKey)
		}
		if p.Filter != nil {
			c.write(" filter (")
			c.expr(p.Filter, "")
			c.write(")")
		}
	case *dag.HTTPScan:
		c.next()
		c.write("get %s", p.URL)
	case *dag.PoolScan:
		c.next()
		c.write("pool %s", p.ID)
	case *dag.PoolMetaScan:
		c.next()
		c.write("pool %s:%s", p.ID, p.Meta)
	case *dag.CommitMetaScan:
		c.next()
		c.write("pool %s@%s:%s", p.Pool, p.Commit, p.Meta)
		if p.Tap {
			c.write(" tap")
		}
	case *dag.LakeMetaScan:
		c.next()
		c.write(":%s", p.Meta)
	case *dag.Pass:
		c.next()
		c.write("pass")
	default:
		c.next()
		c.open("unknown proc: %T", p)
		c.close()
	}
}

func (c *canonDAG) over(o *dag.Over) {
	c.next()
	c.write("over ")
	c.exprs(o.Exprs)
	if len(o.Defs) > 0 {
		c.write(" with ")
		for i, d := range o.Defs {
			if i > 0 {
				c.write(", ")
			}
			c.write("%s=", d.Name)
			c.expr(d.Expr, "")
		}
	}
	if o.Body != nil {
		c.write(" => (")
		c.open()
		c.head = true
		c.seq(o.Body)
		c.close()
		c.ret()
		c.flush()
		c.write(")")
	}
}

func (c *canonDAG) scope(s *dag.Scope) {
	first := c.first
	if !first {
		c.open("(")
		c.ret()
		c.flush()
	}
	for _, d := range s.Consts {
		c.write("const %s = ", d.Name)
		c.expr(d.Expr, "")
		c.ret()
		c.flush()
	}
	for _, f := range s.Funcs {
		c.write("func %s(", f.Name)
		for i := range f.Params {
			if i != 0 {
				c.write(", ")
			}
			c.write(f.Params[i])
		}
		c.open("): (")
		c.ret()
		c.expr(f.Expr, f.Name)
		c.close()
		c.ret()
		c.flush()
		c.write(")")
		c.ret()
		c.flush()
	}
	c.head = true
	c.seq(s.Body)
	if !first {
		c.close()
		c.ret()
		c.flush()
		c.write(")")
	}
}

func isDAGTrue(e dag.Expr) bool {
	if p, ok := e.(*astzed.Primitive); ok {
		return p.Type == "bool" && p.Text == "true"
	}
	return false
}
