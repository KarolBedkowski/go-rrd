package main

import (
	"fmt"
	"github.com/codegangsta/cli"
	"os"
)

func main() {
	app := cli.NewApp()
	app.Name = "boom"
	app.Usage = "RRD CLI tool"
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "filename, f",
			Value: "",
			Usage: "RDD file name",
		},
	}
	app.Commands = []cli.Command{
		{
			Name:  "init",
			Usage: "init RRD file",
			Flags: []cli.Flag{
				cli.IntFlag{
					Name:  "rows",
					Value: 0,
					Usage: "number of rows",
				},
				cli.IntFlag{
					Name:  "cols",
					Value: 0,
					Usage: "number of cols",
				},
				cli.IntFlag{
					Name:  "step",
					Value: 0,
					Usage: "interval between rows (in sec)",
				},
				cli.StringFlag{
					Name:  "function",
					Value: "average",
					Usage: "function applied for data matching one interval (average, sum, min, max, count)",
				},
			},
			Action: initDB,
		},
		{
			Name:    "put",
			Aliases: []string{"p"},
			Usage:   "put value to RRD file",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "ts",
					Value: "",
					Usage: "time stamp (in sec, date, N/now/NOW)",
				},
				cli.IntFlag{
					Name:  "col",
					Value: 0,
					Usage: "destination column",
				},
				cli.Float64Flag{
					Name:  "value",
					Value: 0,
					Usage: "value to insert",
				},
			},
			Action: putValue,
		},
		{
			Name:  "put-values",
			Usage: "put many values into db (as args)",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "ts",
					Value: "",
					Usage: "time stamp (in sec, date, N/now/NOW)",
				},
			},
			Action: putValues,
		},
		{
			Name:    "get",
			Aliases: []string{"g"},
			Usage:   "get value from RRD file",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "ts",
					Value: "",
					Usage: "time stamp (in sec, date, N/now/NOW)",
				},
				cli.IntFlag{
					Name:  "col",
					Value: 0,
					Usage: "destination column",
				},
			},
			Action: getValue,
		},
		{
			Name:    "get-range",
			Aliases: []string{"gr"},
			Usage:   "get values from RRD file",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "ts-min",
					Value: "",
					Usage: "time stamp (in sec, date, N/now/NOW)",
				},
				cli.StringFlag{
					Name:  "ts-max",
					Value: "",
					Usage: "destination column",
				},
			},
			Action: getRangeValues,
		},
		{
			Name:   "info",
			Usage:  "show informations about rddfile",
			Action: showInfo,
		},
		{
			Name:   "last",
			Usage:  "get last time stamp from database",
			Action: showLast,
		},
	}
	app.Run(os.Args)
}

func getFilenameParam(c *cli.Context) (string, bool) {
	filename := c.GlobalString("filename")
	if !c.GlobalIsSet("filename") || filename == "" {
		fmt.Println("Missing database file name")
		return "", false
	}
	return filename, true
}
