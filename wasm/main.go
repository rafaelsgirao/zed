package main

import (
	"context"
	"log"
	"os"
	"strings"

	"github.com/brimdata/zed"
	"github.com/brimdata/zed/compiler"
	"github.com/brimdata/zed/pkg/storage"
	"github.com/brimdata/zed/runtime"
	"github.com/brimdata/zed/zbuf"
	"github.com/brimdata/zed/zio"
	"github.com/brimdata/zed/zio/anyio"
)

//go:generate env GOOS=js GOARCH=wasm go build -o zed.wasm -tags noasm .

func main() {
	filter, input := os.Args[1], os.Args[2]

	flowgraph, err := compiler.Parse(filter)
	if err != nil {
		log.Fatal(err)
	}

	zctx := zed.NewContext()
	zr, err := anyio.NewReader(zctx, strings.NewReader(input))
	if err != nil {
		log.Fatal(err)
	}
	defer zr.Close()

	zwc, err := anyio.NewWriter(os.Stdout, anyio.WriterOpts{Format: "zjson"})
	if err != nil {
		log.Fatal(err)
	}
	defer zwc.Close()

	local := storage.NewLocalEngine()
	comp := compiler.NewFileSystemCompiler(local)
	query, err := runtime.CompileQuery(context.Background(), zctx, comp, flowgraph, []zio.Reader{zr})
	if err != nil {
		log.Fatal(err)
	}
	defer query.Pull(true)

	if err := zbuf.CopyPuller(zwc, query); err != nil {
		log.Fatal(err)
	}
	if err := zwc.Close(); err != nil {
		log.Fatal(err)
	}
}
