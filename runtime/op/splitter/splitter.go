package splitter

import (
	"fmt"
	"sync"

	"github.com/brimdata/zed/lake/commits"
	"github.com/brimdata/zed/lake/data"
	"github.com/brimdata/zed/runtime/op"
	"github.com/brimdata/zed/zbuf"
	"github.com/brimdata/zed/zson"
)

func New(octx *op.Context, parent zbuf.Puller) (scalar, vector zbuf.Puller, err error) {
	snapshotAble, ok := parent.(interface{ Snapshot() commits.View })
	if !ok {
		return nil, nil, fmt.Errorf("internal error: splitter parent has no Snapshot method: %#v", parent)
	}
	s := &splitter{
		parent:   parent,
		snap:     snapshotAble.Snapshot(),
		doneCh:   make(chan struct{}),
		scalarCh: make(chan op.Result),
		vectorCh: make(chan op.Result),
	}
	return &puller{s, s.scalarCh}, &puller{s, s.vectorCh}, nil
}

type splitter struct {
	parent zbuf.Puller
	snap   commits.View

	doneCh   chan struct{}
	scalarCh chan op.Result
	vectorCh chan op.Result

	once sync.Once
}

func (s *splitter) close(err error) {
	go func() {
		r := op.Result{Err: err}
		s.scalarCh <- r
		s.vectorCh <- r
	}()

}
func (s *splitter) run() {
	defer s.parent.Pull(true)
	defer close(s.scalarCh)
	defer close(s.vectorCh)
	for {
		batch, err := s.parent.Pull(false)
		if batch == nil || err != nil {
			s.close(err)
			return
		}
		vals := batch.Values()
		if len(vals) != 1 {
			s.close(fmt.Errorf("internal error: splitter received batch with %d values", len(vals)))
			return
		}
		var object data.Object
		if err := zson.UnmarshalZNG(&vals[0], &object); err != nil {
			s.close(err)
			return
		}
		ch := s.scalarCh
		if s.snap.HasVector(object.ID) {
			ch = s.vectorCh
		}
		select {
		case ch <- op.Result{Batch: batch}:
		case <-s.doneCh:
			s.close(nil)
			return
		}
	}
}

type puller struct {
	splitter *splitter
	ch       chan op.Result
}

func (p *puller) Pull(done bool) (zbuf.Batch, error) {
	p.splitter.once.Do(p.splitter.run)
	if done {
		close(p.splitter.doneCh)
		go func() {
			for range p.ch {
			}
		}()
		return nil, nil
	}
	result := <-p.ch
	return result.Batch, result.Err
}
