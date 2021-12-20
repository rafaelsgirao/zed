package function

import (
	"encoding/base64"
	"encoding/hex"

	"github.com/brimdata/zed"
	"github.com/brimdata/zed/zcode"
)

// https://github.com/brimdata/zed/blob/main/docs/language/functions.md#from_base64
type FromBase64 struct{}

func (f *FromBase64) Call(ctx zed.Allocator, args []zed.Value) *zed.Value {
	zv := args[0]
	if !zv.IsStringy() {
		return newErrorf(ctx, "from_base64: string argument required")
	}
	if zv.Bytes == nil {
		return zed.NullTypeType
	}
	s, _ := zed.DecodeString(zv.Bytes)
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		panic(err)
	}
	return ctx.NewValue(zed.TypeBytes, zed.EncodeBytes(b))
}

// https://github.com/brimdata/zed/blob/main/docs/language/functions.md#to_base64
type ToBase64 struct{}

func (t *ToBase64) Call(ctx zed.Allocator, args []zed.Value) *zed.Value {
	zv := args[0]
	if !zv.IsStringy() {
		return ctx.CopyValue(zed.NewErrorf("to_base64: string argument required"))
	}
	if zv.Bytes == nil {
		return zed.NullString
	}
	return newString(ctx, base64.StdEncoding.EncodeToString(zv.Bytes))
}

// https://github.com/brimdata/zed/blob/main/docs/language/functions.md#from_hex
type FromHex struct{}

func (f *FromHex) Call(ctx zed.Allocator, args []zed.Value) *zed.Value {
	zv := args[0]
	if !zv.IsStringy() {
		return newErrorf(ctx, "to_base64: string argument required")
	}
	if zv.Bytes == nil {
		return zed.NullString
	}
	b, err := hex.DecodeString(string(zv.Bytes))
	if err != nil {
		panic(err)
	}
	return ctx.NewValue(zed.TypeBytes, zcode.Bytes(b))
}

// https://github.com/brimdata/zed/blob/main/docs/language/functions.md#to_hex
type ToHex struct{}

func (t *ToHex) Call(ctx zed.Allocator, args []zed.Value) *zed.Value {
	zv := args[0]
	if zv.Bytes == nil {
		return zed.NullBytes
	}
	return newString(ctx, hex.EncodeToString(zv.Bytes))
}
