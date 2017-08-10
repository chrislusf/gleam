package gio

import (
	"os"

	"github.com/chrislusf/gleam/util"
)

// Emit encode and write a row of data to os.Stdout
func Emit(anyObject ...interface{}) error {
	return TsEmit(util.Now(), anyObject...)
}

// TsEmit encode and write a row of data to os.Stdout
// with ts in milliseconds epoch time
func TsEmit(ts int64, anyObject ...interface{}) error {
	stat.Stats[0].OutputCounter++
	return util.NewRow(ts, anyObject...).WriteTo(os.Stdout)
}

func TsEmitKV(ts int64, keys, values []interface{}) error {
	stat.Stats[0].OutputCounter++
	return util.NewRow(ts).AppendKey(keys...).AppendValue(values...).WriteTo(os.Stdout)
}
