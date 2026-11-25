package main

// A tiny dependency-free SVG chart renderer, so benchmark charts can be
// regenerated with nothing but Go and render directly on GitHub.
//
// Design rules: one y-axis per chart, zero-based, light recessive grid,
// thin marks, fixed categorical colour order, a legend whenever there is more
// than one series, direct end labels, and a <title> tooltip on every mark.
// Colours adapt to dark mode via prefers-color-scheme.

import (
	"fmt"
	"html"
	"math"
	"strings"
)

type series struct {
	name   string
	values []float64
}

const (
	chartW, chartH = 720, 400
	padL, padR     = 72, 96
	padT, padB     = 64, 56
)

const chartStyle = `<style>
  .bg{fill:#fcfcfb} .t1{fill:#0b0b0b} .t2{fill:#52514e} .grid{stroke:#e4e3df} .axis{stroke:#a3a29c}
  .s0{fill:#2a78d6;stroke:#2a78d6} .s1{fill:#eb6834;stroke:#eb6834}
  .ring{stroke:#fcfcfb}
  @media (prefers-color-scheme: dark){
    .bg{fill:#1a1a19} .t1{fill:#f0efec} .t2{fill:#c3c2b7} .grid{stroke:#33332f} .axis{stroke:#6b6a64}
    .s0{fill:#3987e5;stroke:#3987e5} .s1{fill:#d95926;stroke:#d95926} .ring{stroke:#1a1a19}
  }
  text{font-family:-apple-system,"Segoe UI",Helvetica,Arial,sans-serif}
</style>`

// niceMax rounds up to 1, 2, 2.5 or 5 times a power of ten.
func niceMax(v float64) float64 {
	if v <= 0 {
		return 1
	}
	exp := math.Pow(10, math.Floor(math.Log10(v)))
	for _, m := range []float64{1, 2, 2.5, 5, 10} {
		if m*exp >= v {
			return m * exp
		}
	}
	return 10 * exp
}

func frame(title, xLabel, yLabel string, cats []string, ss []series, format string) (*strings.Builder, func(float64) float64, func(int) float64, float64) {
	maxV := 0.0
	for _, s := range ss {
		for _, v := range s.values {
			maxV = math.Max(maxV, v)
		}
	}
	top := niceMax(maxV * 1.08)
	plotW := float64(chartW - padL - padR)
	plotH := float64(chartH - padT - padB)
	y := func(v float64) float64 { return float64(padT) + plotH*(1-v/top) }
	step := plotW / float64(len(cats))
	x := func(i int) float64 { return float64(padL) + step*(float64(i)+0.5) }

	b := &strings.Builder{}
	fmt.Fprintf(b, `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 %d %d" width="%d" height="%d" role="img" aria-label="%s">`,
		chartW, chartH, chartW, chartH, html.EscapeString(title))
	b.WriteString(chartStyle)
	fmt.Fprintf(b, `<rect class="bg" width="%d" height="%d" rx="8"/>`, chartW, chartH)
	fmt.Fprintf(b, `<text class="t1" x="%d" y="28" font-size="16" font-weight="600">%s</text>`, padL-56, html.EscapeString(title))

	// Legend (only for 2+ series), top-left under the title.
	if len(ss) > 1 {
		lx := float64(padL - 56)
		for i, s := range ss {
			fmt.Fprintf(b, `<rect class="s%d" x="%.0f" y="40" width="12" height="12" rx="3"/>`, i, lx)
			fmt.Fprintf(b, `<text class="t2" x="%.0f" y="50" font-size="12">%s</text>`, lx+18, html.EscapeString(s.name))
			lx += 18 + float64(len(s.name))*7 + 24
		}
	}

	// Grid and y ticks.
	for k := 0; k <= 4; k++ {
		v := top * float64(k) / 4
		yy := y(v)
		cls := "grid"
		if k == 0 {
			cls = "axis"
		}
		fmt.Fprintf(b, `<line class="%s" x1="%d" x2="%d" y1="%.1f" y2="%.1f" stroke-width="1"/>`, cls, padL, chartW-padR, yy, yy)
		fmt.Fprintf(b, `<text class="t2" x="%d" y="%.1f" font-size="11" text-anchor="end">%s</text>`, padL-8, yy+4, fmt.Sprintf(format, v))
	}
	// X categories and axis titles.
	for i, c := range cats {
		fmt.Fprintf(b, `<text class="t2" x="%.1f" y="%d" font-size="12" text-anchor="middle">%s</text>`, x(i), chartH-padB+20, html.EscapeString(c))
	}
	if xLabel != "" {
		fmt.Fprintf(b, `<text class="t2" x="%.1f" y="%d" font-size="12" text-anchor="middle">%s</text>`,
			float64(padL)+plotW/2, chartH-12, html.EscapeString(xLabel))
	}
	fmt.Fprintf(b, `<text class="t2" transform="translate(16 %.1f) rotate(-90)" font-size="12" text-anchor="middle">%s</text>`,
		float64(padT)+plotH/2, html.EscapeString(yLabel))
	return b, y, x, step
}

// lineChart draws one 2px line per series over evenly spaced categories.
func lineChart(title, xLabel, yLabel string, cats []string, ss []series, format string) string {
	b, y, x, _ := frame(title, xLabel, yLabel, cats, ss, format)
	for si, s := range ss {
		var pts []string
		for i, v := range s.values {
			pts = append(pts, fmt.Sprintf("%.1f,%.1f", x(i), y(v)))
		}
		fmt.Fprintf(b, `<polyline class="s%d" style="fill:none" stroke-width="2" stroke-linejoin="round" points="%s"/>`, si, strings.Join(pts, " "))
		for i, v := range s.values {
			fmt.Fprintf(b, `<circle class="s%d ring" cx="%.1f" cy="%.1f" r="4.5" stroke-width="2"><title>%s, %s: %s</title></circle>`,
				si, x(i), y(v), html.EscapeString(s.name), html.EscapeString(cats[i]), fmt.Sprintf(format, v))
		}
		// Direct label at the end of each line.
		last := len(s.values) - 1
		fmt.Fprintf(b, `<text class="t1" x="%.1f" y="%.1f" font-size="12">%s</text>`, x(last)+10, y(s.values[last])+4, html.EscapeString(s.name))
	}
	b.WriteString(`</svg>`)
	return b.String()
}

// barChart draws grouped bars with rounded tops and a value label on each.
func barChart(title, xLabel, yLabel string, cats []string, ss []series, format string) string {
	b, y, x, step := frame(title, xLabel, yLabel, cats, ss, format)
	group := math.Min(step*0.6, 64*float64(len(ss)))
	gap := 2.0
	bw := (group - gap*float64(len(ss)-1)) / float64(len(ss))
	base := y(0)
	for si, s := range ss {
		for i, v := range s.values {
			x0 := x(i) - group/2 + float64(si)*(bw+gap)
			top := y(v)
			h := base - top
			r := math.Min(4, h)
			// Path: rounded top corners, square at the baseline.
			fmt.Fprintf(b, `<path class="s%d" d="M%.1f %.1f V%.1f Q%.1f %.1f %.1f %.1f H%.1f Q%.1f %.1f %.1f %.1f V%.1f Z"><title>%s, %s: %s</title></path>`,
				si, x0, base, top+r, x0, top, x0+r, top, x0+bw-r, x0+bw, top, x0+bw, top+r, base,
				html.EscapeString(s.name), html.EscapeString(cats[i]), fmt.Sprintf(format, v))
			fmt.Fprintf(b, `<text class="t1" x="%.1f" y="%.1f" font-size="11" text-anchor="middle">%s</text>`, x0+bw/2, top-6, fmt.Sprintf(format, v))
		}
	}
	b.WriteString(`</svg>`)
	return b.String()
}
