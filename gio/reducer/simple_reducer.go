package reducer

import (
	"github.com/chrislusf/gleam/gio"
)

var (
	SumInt64   = gio.RegisterReducer(sumInt64)
	SumFloat64 = gio.RegisterReducer(sumFloat64)
)

func sumInt64(x, y interface{}) (interface{}, error) {
	return gio.ToInt64(x) + gio.ToInt64(y), nil
}

func sumFloat64(x, y interface{}) (interface{}, error) {
	return gio.ToFloat64(x) + gio.ToFloat64(y), nil
}
