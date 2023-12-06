package commits

import (
	"errors"
	"fmt"
	"io"

	"github.com/brimdata/zed/lake/data"
	"github.com/brimdata/zed/lake/index"
	"github.com/brimdata/zed/order"
	"github.com/brimdata/zed/runtime/expr/extent"
	"github.com/brimdata/zed/zngbytes"
	"github.com/brimdata/zed/zson"
	"github.com/segmentio/ksuid"
)

var ErrWriteConflict = errors.New("write conflict")

type View interface {
	Lookup(ksuid.KSUID) (*data.Object, error)
	LookupIndex(ksuid.KSUID, ksuid.KSUID) (*index.Object, error)
	LookupIndexObjectRules(ksuid.KSUID) ([]index.Rule, error)
	HasVector(ksuid.KSUID) bool
	Select(extent.Span, order.Which) DataObjects
	SelectAll() DataObjects
	SelectIndexes(extent.Span, order.Which) []*index.Object
	SelectAllIndexes() []*index.Object
}

type Writeable interface {
	View
	AddDataObject(*data.Object) error
	DeleteObject(ksuid.KSUID) error
	AddIndexObject(*index.Object) error
	DeleteIndexObject(ksuid.KSUID, ksuid.KSUID) error
	AddVector(ksuid.KSUID) error
	DeleteVector(ksuid.KSUID) error
}

// A snapshot summarizes the pool state at any point in
// the commit object tree.
// XXX redefine snapshot as type map instead of struct
type Snapshot struct {
	objects map[ksuid.KSUID]*data.Object
	indexes index.Map
	vectors map[ksuid.KSUID]struct{}
}

var _ View = (*Snapshot)(nil)
var _ Writeable = (*Snapshot)(nil)

func NewSnapshot() *Snapshot {
	return &Snapshot{
		objects: make(map[ksuid.KSUID]*data.Object),
		indexes: make(index.Map),
		vectors: make(map[ksuid.KSUID]struct{}),
	}
}

func (s *Snapshot) AddDataObject(object *data.Object) error {
	id := object.ID
	if _, ok := s.objects[id]; ok {
		return fmt.Errorf("%s: add of a duplicate data object: %w", id, ErrWriteConflict)
	}
	s.objects[id] = object
	return nil
}

func (s *Snapshot) DeleteObject(id ksuid.KSUID) error {
	if _, ok := s.objects[id]; !ok {
		return fmt.Errorf("%s: delete of a non-existent data object: %w", id, ErrWriteConflict)
	}
	delete(s.objects, id)
	return nil
}

func (s *Snapshot) AddIndexObject(object *index.Object) error {
	id := object.ID
	if s.indexes.Lookup(object.Rule.RuleID(), id) != nil {
		return fmt.Errorf("%s: add of a duplicate index object: %w", id, ErrWriteConflict)
	}
	s.indexes.Insert(object)
	return nil
}

func (s *Snapshot) DeleteIndexObject(ruleID ksuid.KSUID, id ksuid.KSUID) error {
	object := s.indexes.Lookup(ruleID, id)
	if object == nil {
		return fmt.Errorf("%s: delete of a non-existent index object: %w", index.ObjectName(ruleID, id), ErrWriteConflict)
	}
	s.indexes.Delete(ruleID, id)
	return nil
}

func (s *Snapshot) AddVector(id ksuid.KSUID) error {
	if _, ok := s.vectors[id]; ok {
		return fmt.Errorf("%s: add of a duplicate vector of data object: %w", id, ErrWriteConflict)
	}
	s.vectors[id] = struct{}{}
	return nil
}

func (s *Snapshot) DeleteVector(id ksuid.KSUID) error {
	if _, ok := s.vectors[id]; !ok {
		return fmt.Errorf("%s: delete of a non-present vector: %w", id, ErrWriteConflict)
	}
	delete(s.vectors, id)
	return nil
}

func Exists(view View, id ksuid.KSUID) bool {
	_, err := view.Lookup(id)
	return err == nil
}

func (s *Snapshot) Exists(id ksuid.KSUID) bool {
	return Exists(s, id)
}

func (s *Snapshot) Lookup(id ksuid.KSUID) (*data.Object, error) {
	o, ok := s.objects[id]
	if !ok {
		return nil, fmt.Errorf("%s: %w", id, ErrNotFound)
	}
	return o, nil
}

func IndexExists(view View, ruleID, id ksuid.KSUID) bool {
	_, err := view.LookupIndex(ruleID, id)
	return err == nil
}

func (s *Snapshot) LookupIndex(ruleID, id ksuid.KSUID) (*index.Object, error) {
	if o := s.indexes.Lookup(ruleID, id); o != nil {
		return o, nil
	}
	return nil, fmt.Errorf("%s: %w", index.ObjectName(ruleID, id), ErrNotFound)
}

func (s *Snapshot) LookupIndexObjectRules(id ksuid.KSUID) ([]index.Rule, error) {
	r, ok := s.indexes[id]
	if !ok {
		return nil, fmt.Errorf("%s: %w", id, ErrNotFound)
	}
	return r.Rules(), nil
}

func (s *Snapshot) HasVector(id ksuid.KSUID) bool {
	_, ok := s.vectors[id]
	return ok
}

func (s *Snapshot) Select(scan extent.Span, order order.Which) DataObjects {
	var objects DataObjects
	for _, o := range s.objects {
		segspan := o.Span(order)
		if scan == nil || segspan == nil || extent.Overlaps(scan, segspan) {
			objects = append(objects, o)
		}
	}
	return objects
}

func (s *Snapshot) SelectAll() DataObjects {
	var objects DataObjects
	for _, o := range s.objects {
		objects = append(objects, o)
	}
	return objects
}

func (s *Snapshot) SelectIndexes(scan extent.Span, order order.Which) []*index.Object {
	var indexes []*index.Object
	for _, i := range s.indexes.All() {
		o, ok := s.objects[i.ID]
		if !ok {
			continue
		}
		segspan := o.Span(order)
		if scan == nil || segspan == nil || extent.Overlaps(scan, segspan) {
			indexes = append(indexes, i)
		}
	}
	return indexes
}

func (s *Snapshot) SelectAllIndexes() []*index.Object {
	return s.indexes.All()
}

func (s *Snapshot) Unindexed(rules []index.Rule) map[ksuid.KSUID][]index.Rule {
	unindexed := make(map[ksuid.KSUID][]index.Rule)
	for id := range s.objects {
		to := rules
		if o, ok := s.indexes[id]; ok {
			to = o.Missing(rules)
		}
		if len(to) > 0 {
			unindexed[id] = to
		}
	}
	return unindexed
}

func (s *Snapshot) Copy() *Snapshot {
	out := NewSnapshot()
	for key, val := range s.objects {
		out.objects[key] = val
	}
	for key := range s.vectors {
		out.vectors[key] = struct{}{}
	}
	out.indexes = s.indexes.Copy()
	return out
}

// serialize serializes a snapshot as a sequence of actions.  Commit IDs are
// omitted from actions since they are neither available here nor required
// during deserialization.  Deleted entities are serialized as an add-delete
// sequence to meet the requirements of DeleteObject and DeleteIndexObject.
func (s *Snapshot) serialize() ([]byte, error) {
	zs := zngbytes.NewSerializer()
	zs.Decorate(zson.StylePackage)
	for _, o := range s.objects {
		if err := zs.Write(&Add{Object: *o}); err != nil {
			return nil, err
		}
	}
	for id := range s.vectors {
		if err := zs.Write(&AddVector{ID: id}); err != nil {
			return nil, err
		}
	}
	for _, objectRule := range s.indexes {
		for _, o := range objectRule {
			if err := zs.Write(&AddIndex{Object: *o}); err != nil {
				return nil, err
			}
		}
	}
	if err := zs.Close(); err != nil {
		return nil, err
	}
	return zs.Bytes(), nil
}

func decodeSnapshot(r io.Reader) (*Snapshot, error) {
	s := NewSnapshot()
	zd := zngbytes.NewDeserializer(r, ActionTypes)
	defer zd.Close()
	for {
		entry, err := zd.Read()
		if err != nil {
			return nil, err
		}
		if entry == nil {
			return s, nil
		}
		action, ok := entry.(Action)
		if !ok {
			return nil, fmt.Errorf("internal error: corrupt snapshot contains unknown entry type %T", entry)
		}
		if err := PlayAction(s, action); err != nil {
			return nil, err
		}
	}
}

type DataObjects []*data.Object

func (d *DataObjects) Append(objects DataObjects) {
	*d = append(*d, objects...)
}

func PlayAction(w Writeable, action Action) error {
	switch action := action.(type) {
	case *Add:
		return w.AddDataObject(&action.Object)
	case *Delete:
		return w.DeleteObject(action.ID)
	case *AddIndex:
		return w.AddIndexObject(&action.Object)
	case *DeleteIndex:
		return w.DeleteIndexObject(action.RuleID, action.ID)
	case *AddVector:
		return w.AddVector(action.ID)
	case *DeleteVector:
		return w.DeleteVector(action.ID)
	case *Commit:
		// ignore
		return nil
	}
	return fmt.Errorf("lake.commits.PlayAction: unknown action %T", action)
}

// Play "plays" a recorded transaction into a writeable snapshot.
func Play(w Writeable, o *Object) error {
	for _, a := range o.Actions {
		if err := PlayAction(w, a); err != nil {
			return err
		}
	}
	return nil
}

func Vectors(view View) *Snapshot {
	snap := NewSnapshot()
	for _, o := range view.SelectAll() {
		if view.HasVector(o.ID) {
			snap.AddDataObject(o)
		}
	}
	return snap
}
