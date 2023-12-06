package commits

import (
	"errors"
	"fmt"
	"strings"

	"github.com/brimdata/zed"
	"github.com/brimdata/zed/lake/data"
	"github.com/brimdata/zed/lake/index"
	"github.com/brimdata/zed/order"
	"github.com/brimdata/zed/runtime/expr/extent"
	"github.com/segmentio/ksuid"
)

// A Patch represents a difference between a base snapshot and the patched
// snapshot.  Patch implements View so either a patch or a base snapshot
// can be traversed in the same manner.  Furthermore, patches can be easily
// chained to implement a sequence of patches to a base snapshot.
type Patch struct {
	base           View
	diff           *Snapshot
	deletedObjects []ksuid.KSUID
	deletedIndexes []indexRef
	deletedVectors []ksuid.KSUID
}

var _ View = (*Patch)(nil)
var _ Writeable = (*Patch)(nil)

func NewPatch(base View) *Patch {
	return &Patch{
		base: base,
		diff: NewSnapshot(),
	}
}

func (p *Patch) Lookup(id ksuid.KSUID) (*data.Object, error) {
	if s, err := p.diff.Lookup(id); err == nil {
		return s, nil
	}
	return p.base.Lookup(id)
}

func (p *Patch) LookupIndex(ruleID, id ksuid.KSUID) (*index.Object, error) {
	if s, err := p.diff.LookupIndex(ruleID, id); err == nil {
		return s, nil
	}
	return p.base.LookupIndex(ruleID, id)
}

func (p *Patch) LookupIndexObjectRules(id ksuid.KSUID) ([]index.Rule, error) {
	if r, err := p.diff.LookupIndexObjectRules(id); err == nil {
		return r, nil
	}
	return p.base.LookupIndexObjectRules(id)
}

func (p *Patch) HasVector(id ksuid.KSUID) bool {
	return p.diff.HasVector(id) || p.base.HasVector(id)
}

func (p *Patch) Select(span extent.Span, o order.Which) DataObjects {
	objects := p.base.Select(span, o)
	objects.Append(p.diff.Select(span, o))
	return objects
}

func (p *Patch) SelectAll() DataObjects {
	objects := p.base.SelectAll()
	objects.Append(p.diff.SelectAll())
	return objects
}

func (p *Patch) SelectIndexes(span extent.Span, o order.Which) []*index.Object {
	objects := p.base.SelectIndexes(span, o)
	return append(objects, p.diff.SelectIndexes(span, o)...)
}

func (p *Patch) SelectAllIndexes() []*index.Object {
	return append(p.base.SelectAllIndexes(), p.diff.SelectAllIndexes()...)
}

func (p *Patch) DataObjects() []ksuid.KSUID {
	var ids []ksuid.KSUID
	for _, dataObject := range p.diff.SelectAll() {
		ids = append(ids, dataObject.ID)
	}
	return ids
}

func (p *Patch) AddDataObject(object *data.Object) error {
	if Exists(p.base, object.ID) {
		return ErrExists
	}
	return p.diff.AddDataObject(object)
}

func (p *Patch) DeleteObject(id ksuid.KSUID) error {
	if p.diff.Exists(id) {
		return p.diff.DeleteObject(id)
	}
	if !Exists(p.base, id) {
		return ErrNotFound
	}
	// Keep track of the deletions from the base so we can add the
	// needed delete Actions when building the transaction patch.
	p.deletedObjects = append(p.deletedObjects, id)
	return nil
}

func (p *Patch) AddIndexObject(object *index.Object) error {
	if IndexExists(p.base, object.Rule.RuleID(), object.ID) {
		return ErrExists
	}
	return p.diff.AddIndexObject(object)
}

type indexRef struct {
	id     ksuid.KSUID
	ruleID ksuid.KSUID
}

func (i indexRef) Key() string {
	var b strings.Builder
	b.Write(i.id.Bytes())
	b.Write(i.ruleID.Bytes())
	return b.String()
}

func (p *Patch) DeleteIndexObject(ruleID ksuid.KSUID, id ksuid.KSUID) error {
	if IndexExists(p.diff, ruleID, id) {
		return p.diff.DeleteIndexObject(ruleID, id)
	}
	if !IndexExists(p.base, ruleID, id) {
		return ErrNotFound
	}
	// Keep track of the deletions from the base so we can add the
	// needed delete Actions when building the transaction patch.
	p.deletedIndexes = append(p.deletedIndexes, indexRef{id, ruleID})
	return nil
}

func (p *Patch) AddVector(id ksuid.KSUID) error {
	if p.HasVector(id) {
		return ErrExists
	}
	return p.diff.AddVector(id)
}

func (p *Patch) DeleteVector(id ksuid.KSUID) error {
	if p.diff.HasVector(id) {
		return p.diff.DeleteVector(id)
	}
	if !p.base.HasVector(id) {
		return ErrNotFound
	}
	// Keep track of the deletions from the base so we can add the
	// needed delete Actions when building the transaction patch.
	p.deletedVectors = append(p.deletedVectors, id)
	return nil
}

func (p *Patch) NewCommitObject(parent ksuid.KSUID, retries int, author, message string, meta zed.Value) *Object {
	o := NewObject(parent, author, message, meta, retries)
	for _, id := range p.deletedObjects {
		o.appendDelete(id)
	}
	for _, s := range p.diff.objects {
		o.appendAdd(s)
	}
	for _, r := range p.deletedIndexes {
		o.appendDeleteIndex(r.ruleID, r.id)
	}
	for _, s := range p.diff.indexes.All() {
		o.appendAddIndex(s)
	}
	for _, id := range p.deletedVectors {
		o.appendDeleteVector(id)
	}
	for id := range p.diff.vectors {
		o.appendAddVector(id)
	}
	return o
}

func (p *Patch) Revert(tip *Snapshot, commit, parent ksuid.KSUID, retries int, author, message string) (*Object, error) {
	object := NewObject(parent, author, message, *zed.Null, retries)
	// For each data object that is added in the patch and is also in the tip, we do a delete.
	for _, dataObject := range p.diff.SelectAll() {
		if Exists(tip, dataObject.ID) {
			object.appendDelete(dataObject.ID)
		}
	}
	// For each delete in the patch that is absent in the tip, we do an add.
	for _, id := range p.deletedObjects {
		// Reach back to get the object before it was deleted in this patch.
		dataObject, err := p.base.Lookup(id)
		if err != nil {
			return nil, err
		}
		if dataObject == nil {
			return nil, fmt.Errorf("corrupt snapshot: ID %s is in patch's deleted objects but not in patch's base snapshot", id)
		}
		if !Exists(tip, dataObject.ID) {
			object.appendAdd(dataObject)
		}
	}
	// For each index object that was added in the patch that is also in the tip, we do a delete.
	for _, indexObject := range p.diff.indexes.All() {
		if tip.indexes.Exists(indexObject) {
			object.appendDeleteIndex(indexObject.Rule.RuleID(), indexObject.ID)
		}
	}
	// For each deleted index object in the patch that is also not present in the
	// tip, we do an add index.
	for _, ref := range p.deletedIndexes {
		if !IndexExists(tip, ref.ruleID, ref.id) {
			o, err := p.base.LookupIndex(ref.ruleID, ref.id)
			if err != nil {
				return nil, err
			}
			if o == nil {
				return nil, fmt.Errorf("corrupt snapshot: index object %s:%s is in patch's deleted index objects but not in patch's base snapshot", ref.ruleID, ref.id)
			}
			object.appendAddIndex(o)
		}
	}
	if len(object.Actions) == 1 {
		return nil, errors.New("revert commit is empty")
	}
	return object, nil
}

func Diff(parent, child *Patch) (*Patch, error) {
	var dirty bool
	p := NewPatch(parent)
	deletedObjects := make(map[ksuid.KSUID]struct{})
	for _, id := range child.deletedObjects {
		deletedObjects[id] = struct{}{}
	}
	// For each object in the child patch that isn't in the parent, create an add,
	// unless the parent deletes it, then return an error.
	for _, o := range child.SelectAll() {
		if !Exists(parent, o.ID) {
			if _, ok := deletedObjects[o.ID]; ok {
				return nil, fmt.Errorf("parent branch deletes object that child branch adds: %d", o.ID)
			}
			if err := p.AddDataObject(o); err != nil {
				return nil, err
			}
			dirty = true
		}
	}
	// For each delete in the child patch, create a delete.
	// If the object doesn't exist in the parent, then we have a
	// delete conflict.
	for _, id := range child.deletedObjects {
		if Exists(parent, id) {
			if err := p.DeleteObject(id); err != nil {
				return nil, err
			}
			dirty = true
		} else {
			return nil, fmt.Errorf("delete conflict: %s", id)
		}
	}
	deletedIndexes := make(map[string]struct{})
	for _, idx := range child.deletedIndexes {
		deletedIndexes[idx.Key()] = struct{}{}
	}
	// For each index entry in child that isn't in the parent, create an add-index.
	for _, idx := range child.SelectAllIndexes() {
		if !IndexExists(parent, idx.Rule.RuleID(), idx.ID) {
			key := indexRef{id: idx.ID, ruleID: idx.Rule.RuleID()}.Key()
			if _, ok := deletedIndexes[key]; ok {
				return nil, fmt.Errorf("parent branch deletes index object that child branch adds: %s:%s", idx.Rule.RuleID(), idx.ID)
			}
			if err := p.AddIndexObject(idx); err != nil {
				return nil, err
			}
			dirty = true
		} else {
			return nil, fmt.Errorf("delete conflict on index: %s:%s", idx.Rule.RuleID(), idx.ID)
		}
	}
	// For each delete-index in the child patch, create a delete-index.
	// (XXX should return an error if it's not in the parent).
	for _, idx := range child.deletedIndexes {
		if IndexExists(parent, idx.ruleID, idx.id) {
			if err := p.DeleteIndexObject(idx.ruleID, idx.id); err != nil {
				return nil, err
			}
			dirty = true
		}
	}
	if !dirty {
		return nil, errors.New("difference is empty")
	}
	return p, nil
}
