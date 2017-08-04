package reducer

import (
	"github.com/chrislusf/gleam/gio"
)

var (
	Sum = gio.RegisterReducer(sum)
)

func sum(x, y interface{}) (interface{}, error) {
	return gio.ToInt64(x) + gio.ToInt64(y), nil
}
