package anyio

import (
	"fmt"
	"io"

	"github.com/brimdata/zed"
	"github.com/brimdata/zed/zio"
	"github.com/brimdata/zed/zio/arrowio"
	"github.com/brimdata/zed/zio/csvio"
	"github.com/brimdata/zed/zio/jsonio"
	"github.com/brimdata/zed/zio/lakeio"
	"github.com/brimdata/zed/zio/parquetio"
	"github.com/brimdata/zed/zio/tableio"
	"github.com/brimdata/zed/zio/textio"
	"github.com/brimdata/zed/zio/vngio"
	"github.com/brimdata/zed/zio/zeekio"
	"github.com/brimdata/zed/zio/zjsonio"
	"github.com/brimdata/zed/zio/zngio"
	"github.com/brimdata/zed/zio/zsonio"
)

type WriterOpts struct {
	Format string
	Lake   lakeio.WriterOpts
	CSV    csvio.WriterOpts
	VNG    *vngio.WriterOpts // Nil means use defaults via vngio.NewWriter.
	ZNG    *zngio.WriterOpts // Nil means use defaults via zngio.NewWriter.
	ZSON   zsonio.WriterOpts
}

func NewWriter(w io.WriteCloser, opts WriterOpts) (zio.WriteCloser, error) {
	switch opts.Format {
	case "arrows":
		return arrowio.NewWriter(w), nil
	case "csv":
		return csvio.NewWriter(w, opts.CSV), nil
	case "json":
		return jsonio.NewWriter(w), nil
	case "lake":
		return lakeio.NewWriter(w, opts.Lake), nil
	case "null":
		return &nullWriter{}, nil
	case "parquet":
		return parquetio.NewWriter(w), nil
	case "table":
		return tableio.NewWriter(w), nil
	case "text":
		return textio.NewWriter(w), nil
	case "tsv":
		opts.CSV.Delim = '\t'
		return csvio.NewWriter(w, opts.CSV), nil
	case "vng":
		if opts.VNG == nil {
			return vngio.NewWriter(w)
		}
		return vngio.NewWriterWithOpts(w, *opts.VNG)
	case "zeek":
		return zeekio.NewWriter(w), nil
	case "zjson":
		return zjsonio.NewWriter(w), nil
	case "zng":
		if opts.ZNG == nil {
			return zngio.NewWriter(w), nil
		}
		return zngio.NewWriterWithOpts(w, *opts.ZNG), nil
	case "zson", "":
		return zsonio.NewWriter(w, opts.ZSON), nil
	default:
		return nil, fmt.Errorf("unknown format: %s", opts.Format)
	}
}

type nullWriter struct{}

func (*nullWriter) Write(*zed.Value) error {
	return nil
}

func (*nullWriter) Close() error {
	return nil
}
