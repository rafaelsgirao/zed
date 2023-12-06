package anyio

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/brimdata/zed"
	"github.com/brimdata/zed/compiler/optimizer/demand"
	"github.com/brimdata/zed/zio"
	"github.com/brimdata/zed/zio/arrowio"
	"github.com/brimdata/zed/zio/csvio"
	"github.com/brimdata/zed/zio/jsonio"
	"github.com/brimdata/zed/zio/parquetio"
	"github.com/brimdata/zed/zio/vngio"
	"github.com/brimdata/zed/zio/zeekio"
	"github.com/brimdata/zed/zio/zjsonio"
	"github.com/brimdata/zed/zio/zngio"
	"github.com/brimdata/zed/zio/zsonio"
)

type ReaderOpts struct {
	Format string
	CSV    csvio.ReaderOpts
	ZNG    zngio.ReaderOpts
}

func NewReader(zctx *zed.Context, r io.Reader, demandOut demand.Demand) (zio.ReadCloser, error) {
	return NewReaderWithOpts(zctx, r, demandOut, ReaderOpts{})
}

func NewReaderWithOpts(zctx *zed.Context, r io.Reader, demandOut demand.Demand, opts ReaderOpts) (zio.ReadCloser, error) {
	if opts.Format != "" && opts.Format != "auto" {
		return lookupReader(zctx, r, demandOut, opts)
	}

	var parquetErr, vngErr error
	if rs, ok := r.(io.ReadSeeker); ok {
		if n, err := rs.Seek(0, io.SeekCurrent); err == nil {
			var zr zio.Reader
			zr, parquetErr = parquetio.NewReader(zctx, rs)
			if parquetErr == nil {
				return zio.NopReadCloser(zr), nil
			}
			if _, err := rs.Seek(n, io.SeekStart); err != nil {
				return nil, err
			}
			zr, vngErr = vngio.NewReader(zctx, rs, demandOut)
			if vngErr == nil {
				return zio.NopReadCloser(zr), nil
			}
			if _, err := rs.Seek(n, io.SeekStart); err != nil {
				return nil, err
			}
		} else {
			parquetErr = err
			vngErr = err
		}
		parquetErr = fmt.Errorf("parquet: %w", parquetErr)
		vngErr = fmt.Errorf("vng: %w", vngErr)
	} else {
		parquetErr = errors.New("parquet: auto-detection requires seekable input")
		vngErr = errors.New("vng: auto-detection requires seekable input")
	}

	track := NewTrack(r)

	arrowsErr := isArrowStream(track)
	if arrowsErr == nil {
		return arrowio.NewReader(zctx, track.Reader())
	}
	arrowsErr = fmt.Errorf("arrows: %w", arrowsErr)
	track.Reset()

	zeekErr := match(zeekio.NewReader(zed.NewContext(), track), "zeek", 1)
	if zeekErr == nil {
		return zio.NopReadCloser(zeekio.NewReader(zctx, track.Reader())), nil
	}
	track.Reset()

	// ZJSON must come before JSON and ZSON since it is a subset of both.
	zjsonErr := match(zjsonio.NewReader(zed.NewContext(), track), "zjson", 1)
	if zjsonErr == nil {
		return zio.NopReadCloser(zjsonio.NewReader(zctx, track.Reader())), nil
	}
	track.Reset()

	// JSON comes before ZSON because the JSON reader is faster than the
	// ZSON reader.  The number of values wanted is greater than one for the
	// sake of tests.
	jsonErr := match(jsonio.NewReader(zed.NewContext(), track), "json", 10)
	if jsonErr == nil {
		return zio.NopReadCloser(jsonio.NewReader(zctx, track.Reader())), nil
	}
	track.Reset()

	zsonErr := match(zsonio.NewReader(zed.NewContext(), track), "zson", 1)
	if zsonErr == nil {
		return zio.NopReadCloser(zsonio.NewReader(zctx, track.Reader())), nil
	}
	track.Reset()

	// For the matching reader, force validation to true so we are extra
	// careful about auto-matching ZNG.  Then, once matched, relaxed
	// validation to the user setting in the actual reader returned.
	zngOpts := opts.ZNG
	zngOpts.Validate = true
	zngReader := zngio.NewReaderWithOpts(zed.NewContext(), track, zngOpts)
	zngErr := match(zngReader, "zng", 1)
	// Close zngReader to ensure that it does not continue to call track.Read.
	zngReader.Close()
	if zngErr == nil {
		return zngio.NewReaderWithOpts(zctx, track.Reader(), opts.ZNG), nil
	}
	track.Reset()

	csvErr := isCSVStream(track, ',', "csv")
	if csvErr == nil {
		return zio.NopReadCloser(csvio.NewReader(zctx, track.Reader(), csvio.ReaderOpts{Delim: ','})), nil
	}
	track.Reset()

	tsvErr := isCSVStream(track, '\t', "tsv")
	if tsvErr == nil {
		return zio.NopReadCloser(csvio.NewReader(zctx, track.Reader(), csvio.ReaderOpts{Delim: '\t'})), nil
	}
	track.Reset()

	lineErr := errors.New("line: auto-detection not supported")
	return nil, joinErrs([]error{
		arrowsErr,
		csvErr,
		jsonErr,
		lineErr,
		parquetErr,
		tsvErr,
		vngErr,
		zeekErr,
		zjsonErr,
		zngErr,
		zsonErr,
	})
}

func isArrowStream(track *Track) error {
	// Streams created by Arrow 0.15.0 or later begin with a 4-byte
	// continuation indicator (0xffffffff) followed by a 4-byte
	// little-endian schema message length.  Older streams begin with the
	// length.
	buf := make([]byte, 4)
	if _, err := io.ReadFull(track, buf); err != nil {
		return err
	}
	if string(buf) == "\xff\xff\xff\xff" {
		// This looks like a continuation indicator.  Skip it.
		if _, err := io.ReadFull(track, buf); err != nil {
			return err
		}
	}
	if binary.LittleEndian.Uint32(buf) > 1048576 {
		// Prevent arrowio.NewReader from attempting to read an
		// unreasonable amount.
		return errors.New("schema message length exceeds 1 MiB")
	}
	track.Reset()
	zrc, err := arrowio.NewReader(zed.NewContext(), track)
	if err != nil {
		return err
	}
	defer zrc.Close()
	_, err = zrc.Read()
	return err
}

func isCSVStream(track *Track, delim rune, name string) error {
	if s, err := bufio.NewReader(track).ReadString('\n'); err != nil {
		return fmt.Errorf("%s: line 1: %w", name, err)
	} else if !strings.Contains(s, string(delim)) {
		return fmt.Errorf("%s: line 1: delimiter %q not found", name, delim)
	}
	track.Reset()
	return match(csvio.NewReader(zed.NewContext(), track, csvio.ReaderOpts{Delim: delim}), name, 1)
}

func joinErrs(errs []error) error {
	s := "format detection error"
	for _, e := range errs {
		s += "\n\t" + e.Error()
	}
	return errors.New(s)
}

func match(r zio.Reader, name string, want int) error {
	for i := 0; i < want; i++ {
		val, err := r.Read()
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		if val == nil {
			return nil
		}
	}
	return nil
}
