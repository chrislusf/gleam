package gio

import "github.com/chrislusf/gleam/util"

func ToInt64(val interface{}) int64 {
	return util.ToInt64(val)
}
func ToFloat64(val interface{}) float64 {
	return util.ToFloat64(val)
}

func ToString(val interface{}) string {
	return util.ToString(val)
}

func ToBytes(val interface{}) []byte {
	return util.ToBytes(val)
}
