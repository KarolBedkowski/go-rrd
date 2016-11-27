//
// chart.go
// Copyright (C) 2016 Karol Będkowski
//

package main

import (
	"github.com/wcharczuk/go-chart"
	"os"
	"time"
)

func MyTimeMinuteValueFormatter(v interface{}) string {
	return chart.TimeValueFormatterWithFormat(v, "02.01 15:04")
}

type Plot struct {
	Rows           Rows
	Cols           []string
	Width          int
	Height         int
	UseSecoundAxis bool
}

func (p *Plot) plotChart(filename string) {
	series := make([]chart.TimeSeries, 0, len(p.Cols))
	numRows := len(p.Rows)

	ts := chart.TimeSeries{
		XValues: make([]time.Time, 0, numRows),
		YValues: make([]float64, 0, numRows),
		Name:    p.Cols[0],
		Style: chart.Style{
			Show:        true,
			FontColor:   chart.ColorBlue,
			StrokeColor: chart.ColorBlue,
		},
	}
	series = append(series, ts)

	if len(p.Cols) == 2 {
		ts := chart.TimeSeries{
			XValues: make([]time.Time, 0, numRows),
			YValues: make([]float64, 0, numRows),
			Name:    p.Cols[1],
			Style: chart.Style{
				Show:        true,
				FontColor:   chart.ColorGreen,
				StrokeColor: chart.ColorGreen,
			},
		}
		if p.UseSecoundAxis {
			ts.YAxis = chart.YAxisSecondary
		}
		series = append(series, ts)
	}

	for _, row := range p.Rows {
		ts := time.Unix(row.TS, 0)
		for serieNo, col := range row.Values {
			if col.Valid {
				series[serieNo].XValues = append(series[serieNo].XValues, ts)
				series[serieNo].YValues = append(series[serieNo].YValues, float64(col.Value))
			}
		}
	}

	cSeries := make([]chart.Series, 0, len(series))
	for _, s := range series {
		cSeries = append(cSeries, s)
	}

	graph := chart.Chart{
		Width:  p.Width,
		Height: p.Height,
		XAxis: chart.XAxis{
			Style: chart.Style{
				Show: true,
			},
		},
		YAxis: chart.YAxis{
			Style: chart.Style{
				Show: true, //enables / displays the y-axis
			},
		},
		Series: cSeries,
	}

	tsRange := p.Rows[numRows-1].TS - p.Rows[0].TS
	if tsRange < 60*60*24*2 { // 2 days
		graph.XAxis.ValueFormatter = MyTimeMinuteValueFormatter
	}
	if len(p.Cols) > 1 && p.UseSecoundAxis {
		graph.YAxisSecondary = chart.YAxis{
			Style: chart.Style{
				Show: true, //enables / displays the secondary y-axis
			},
		}
	}

	graph.Elements = []chart.Renderable{
		chart.Legend(&graph),
	}

	f, _ := os.Create(filename)
	defer f.Close()
	graph.Render(chart.PNG, f)
}
