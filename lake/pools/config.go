package pools

import (
	"github.com/brimdata/zed/lake/data"
	"github.com/brimdata/zed/lake/journal"
	"github.com/brimdata/zed/order"
	"github.com/brimdata/zed/pkg/field"
	"github.com/brimdata/zed/pkg/nano"
	"github.com/brimdata/zed/pkg/storage"
	"github.com/segmentio/ksuid"
)

type Config struct {
	Ts         nano.Ts       `zed:"ts"`
	Name       string        `zed:"name"`
	ID         ksuid.KSUID   `zed:"id"`
	SortKey    order.SortKey `zed:"layout"`
	SeekStride int           `zed:"seek_stride"`
	Threshold  int64         `zed:"threshold"`
}

var _ journal.Entry = (*Config)(nil)

func NewConfig(name string, sortKey order.SortKey, thresh int64, seekStride int) *Config {
	if sortKey.IsNil() {
		sortKey = order.NewSortKey(order.Desc, field.DottedList("ts"))
	}
	if thresh == 0 {
		thresh = data.DefaultThreshold
	}
	if seekStride == 0 {
		seekStride = data.DefaultSeekStride
	}
	return &Config{
		Ts:         nano.Now(),
		Name:       name,
		ID:         ksuid.New(),
		SortKey:    sortKey,
		SeekStride: seekStride,
		Threshold:  thresh,
	}
}

func (p *Config) Key() string {
	return p.Name
}

func (p *Config) Path(root *storage.URI) *storage.URI {
	return root.JoinPath(p.ID.String())
}
