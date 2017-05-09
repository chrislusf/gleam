package ui

import (
	"fmt"
	"math"

	"github.com/ajstarks/svgo"
)

const (
	m = 10
)
const (
	linesize    = 4
	labelfs     = 16
	notchsize   = 8
	groupmargin = 10

	lcolor      = "rgb(190,190,190)"
	boxradius   = 10
	lopacity    = "1.0"
	defcolor    = "black"
	linefmt     = "stroke:%s;fill:none"
	globalstyle = "font-family:Calibri,sans-serif;font-size:%dpx;fill:black;text-anchor:middle;stroke-linecap:round;stroke-width:%dpx;stroke-opacity:%s"
	ltstyle     = "text-anchor:%s;fill:black"
	legendstyle = "text-anchor:start;fill:black;font-size:%dpx"
	gridstyle   = "fill:none; stroke:gray; stroke-opacity:0.3"
	notefmt     = "font-size:%dpx"
	ntfmt       = "text-anchor:%s;fill:%s"
)

var ()

type point struct {
	x int
	y int
}

func connect(canvas *svg.SVG, a, b point, label string) {
	downLength := 2 * m
	canvas.Line(a.x, a.y, a.x, a.y+downLength, "stroke-width:1;stroke:black;")
	canvas.Line(a.x, a.y+downLength, b.x, a.y+downLength, "stroke-width:1;stroke:black;")
	linelabel(canvas, b.x, a.y+downLength, b.x, b.y, label, "d", "", "d", "straight", "black")
}

// message object
func message(canvas *svg.SVG, x, y, w, h, l int, bcolor, scolor string) {
	et := h / 3
	w2 := w / 2
	px := w / 8
	py := h / 8
	e1x := []int{x, x, x + w, x + w, x + w2, x}
	e1y := []int{y + et, y + h, y + h, y + et, y + (et * 2), y + et}
	e2x := []int{x, x + w2, x + w, x + w2, x}
	e2y := []int{y + et, y, y + et, y + (et * 2), y + et}

	canvas.Polygon(e2x, e2y, "fill:"+bcolor)
	canvas.Polygon(e1x, e1y, "fill:"+scolor)
	canvas.Roundrect(x+px, y+py, w-(px*2), h-py, l, l, "fill:"+scolor)
	canvas.Line(x, y+et, x+w2, y+(et*2), "stroke-width:1;stroke:"+bcolor)
	canvas.Line(x+w, y+et, x+w2, y+(et*2), "stroke-width:1;stroke:"+bcolor)
}

// linelabel determines the connection and arrow geometry
func linelabel(canvas *svg.SVG, x1, y1, x2, y2 int, label string, mark string, d1 string, d2 string, dir string, color string) {
	aw := linesize * 4
	ah := linesize * 3

	if len(color) == 0 {
		color = lcolor
	}
	switch mark {
	case "b":
		lx1, ly1 := arrow(canvas, x1, y1, aw, ah, d1, color)
		lx2, ly2 := arrow(canvas, x2, y2, aw, ah, d2, color)
		doline(canvas, lx1, ly1, lx2, ly2, linestyle(color), dir, label)

	case "s":
		lx1, ly1 := arrow(canvas, x1, y1, aw, ah, d1, color)
		doline(canvas, lx1, ly1, x2, y2, linestyle(color), dir, label)

	case "d":
		lx2, ly2 := arrow(canvas, x2, y2, aw, ah, d2, color)
		doline(canvas, x1, y1, lx2, ly2, linestyle(color), dir, label)

	default:
		doline(canvas, x1, y1, x2, y2, linestyle(color), dir, label)
	}
}

// linestyle returns the style for lines
func linestyle(color string) string {
	return fmt.Sprintf(linefmt, color)
}

// arrow constructs line-ending arrows according to connecting points
func arrow(canvas *svg.SVG, x, y, w, h int, dir string, color string) (xl, yl int) {
	var xp = []int{x, x, x, x}
	var yp = []int{y, y, y, y}

	n := notchsize
	switch dir {
	case "r":
		xp[1] = x - w
		yp[1] = y - h/2
		xp[2] = (x - w) + n
		yp[2] = y
		xp[3] = x - w
		yp[3] = y + h/2
		xl, yl = xp[2], y
	case "l":
		xp[1] = x + w
		yp[1] = y - h/2
		xp[2] = (x + w) - n
		yp[2] = y
		xp[3] = x + w
		yp[3] = y + h/2
		xl, yl = xp[2], y
	case "u":
		xp[1] = x - w/2
		yp[1] = y + h
		xp[2] = x
		yp[2] = (y + h) - n
		xp[3] = x + w/2
		yp[3] = y + h
		xl, yl = x, yp[2]
	case "d":
		xp[1] = x - w/2
		yp[1] = y - h
		xp[2] = x
		yp[2] = (y - h) + n
		xp[3] = x + w/2
		yp[3] = y - h
		xl, yl = x, yp[2]
	}
	canvas.Polygon(xp, yp, "fill:"+color+";fill-opacity:"+lopacity)
	return xl, yl
}

// doline draws a line between to coordinates
func doline(canvas *svg.SVG, x1, y1, x2, y2 int, style, direction, label string) {
	var labelstyle string
	var upflag bool

	tadjust := 6
	mx := (x2 - x1) / 2
	my := (y2 - y1) / 2
	lx := x1 + mx
	ly := y1 + my
	m, _ := sloper(x1, y1, x2, y2)
	hline := m == 0
	vline := m == math.Inf(-1) || m == math.Inf(1)
	straight := hline || vline

	switch {
	case m < 0: // upwards line
		upflag = true
		labelstyle += "text-anchor:end;"
		lx -= tadjust
	case hline: // horizontal line
		labelstyle += "text-anchor:middle;baseline-shift:20%;"
		ly -= tadjust
	case m > 0: // downwards line
		upflag = false
		labelstyle += "text-anchor:start;"
		lx += tadjust
	}
	if !straight && direction != "straight" {
		cx, cy := x1, y2 // initial control points
		// fmt.Fprintf(os.Stderr, "%s slope = %.3f\n", label, m)
		if upflag {
			if direction == "ccw" {
				cx, cy = x2, y1
			} else {
				cx, cy = x1, y2
			}
		} else {
			if direction == "ccw" {
				cx, cy = x1, y2
			} else {
				cx, cy = x2, y1
			}
		}
		canvas.Qbez(x1, y1, cx, cy, x2, y2, style)
		labelstyle += "text-anchor:middle"
		canvas.Text(lx, ly, label, labelstyle)
	} else {
		canvas.Line(x1, y1, x2, y2, style)
		canvas.Text(lx, ly, label, labelstyle) // midpoint
	}
}

// sloper computes the slope and r of a line
func sloper(x1, y1, x2, y2 int) (m, r float64) {
	dy := float64(y1 - y2)
	dx := float64(x1 - x2)
	m = dy / dx
	r = math.Atan2(dy, dx) * (180 / math.Pi)
	return m, r
}
