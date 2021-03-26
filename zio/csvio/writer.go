package csvio

import (
	"encoding/csv"
	"errors"
	"io"
	"time"

	"github.com/brimsec/zq/proc/fuse"
	"github.com/brimsec/zq/zbuf"
	"github.com/brimsec/zq/zio/tzngio"
	"github.com/brimsec/zq/zng"
	"github.com/brimsec/zq/zng/flattener"
	"github.com/brimsec/zq/zng/resolver"
)

var ErrNotDataFrame = errors.New("csv output requires uniform records but different types encountered")

type Writer struct {
	epochDates bool
	writer     io.WriteCloser
	encoder    *csv.Writer
	flattener  *flattener.Flattener
	format     tzngio.OutFmt
	first      *zng.TypeRecord
}

type WriterOpts struct {
	EpochDates bool
	Fuse       bool
	UTF8       bool
}

func NewWriter(w io.WriteCloser, zctx *resolver.Context, opts WriterOpts) zbuf.WriteCloser {
	format := tzngio.OutFormatZeekAscii
	if opts.UTF8 {
		format = tzngio.OutFormatZeek
	}
	zw := &Writer{
		writer:     w,
		epochDates: opts.EpochDates,
		encoder:    csv.NewWriter(w),
		flattener:  flattener.New(resolver.NewContext()),
		format:     format,
	}
	if opts.Fuse {
		return fuse.WriteCloser(zw, zctx)
	}
	return zw
}

func (w *Writer) Close() error {
	w.encoder.Flush()
	return w.writer.Close()
}

func (w *Writer) Flush() error {
	w.encoder.Flush()
	return w.encoder.Error()
}

func (w *Writer) Write(rec *zng.Record) error {
	rec, err := w.flattener.Flatten(rec)
	if err != nil {
		return err
	}
	if w.first == nil {
		w.first = zng.TypeRecordOf(rec.Type)
		var hdr []string
		for _, col := range rec.Columns() {
			hdr = append(hdr, col.Name)
		}
		if err := w.encoder.Write(hdr); err != nil {
			return err
		}
	} else if rec.Type != w.first {
		return ErrNotDataFrame
	}
	var out []string
	for k, col := range rec.Columns() {
		var v string
		// O(n^2)
		value := rec.ValueByColumn(k)
		if !value.IsUnsetOrNil() {
			switch col.Type.ID() {
			case zng.IdTime:
				ts, err := zng.DecodeTime(value.Bytes)
				if err != nil {
					return err
				}
				if w.epochDates {
					v = ts.StringFloat()
				} else {
					v = ts.Time().UTC().Format(time.RFC3339Nano)
				}
			case zng.IdString, zng.IdBstring, zng.IdType, zng.IdError:
				v = string(value.Bytes)
			default:
				v = tzngio.FormatValue(value, w.format)
			}
		}
		out = append(out, v)
	}
	return w.encoder.Write(out)
}
