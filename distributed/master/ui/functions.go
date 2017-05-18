package ui

import (
	"text/template"
	"time"
)

var (
	funcMap = template.FuncMap{
		"duration": Duration,
		"unix":     Unix,
	}
)

func Duration(stop, start int64) string {
	if stop == 0 {
		stop = int64(time.Now().UnixNano())
	}
	d := stop - start
	return time.Duration(d - d%1e6).String()
}

func Unix(t int64) time.Time {
	nano := t / 1e9
	return time.Unix(t/1e9, nano-nano%1e6)
}
