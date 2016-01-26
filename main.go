package main

import (
	"os"

	"github.com/codegangsta/cli"
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
		cli.BoolFlag{
			Name:  "format-ts",
			Usage: "human-readable time in output",
		},
		cli.StringFlag{
			Name:  "custom-ts-format",
			Value: "2006-01-02T15:04:05Z07:00",
			Usage: "time stamp formatting string",
		},
		cli.StringFlag{
			Name:  "separator",
			Value: "\t",
			Usage: "fields separator",
		},
		cli.BoolFlag{
			Name:  "debug, d",
			Usage: "display debug informations",
		},
	}
	app.Commands = []cli.Command{
		{
			Name:  "init",
			Usage: "init RRD file",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "columns, c",
					Value: "",
					Usage: "columns definition in form: function[:col name],function[:col name],.... Functions: average/avg/sum/min/minimum/max/maximum/count/last",
				},
				cli.StringFlag{
					Name:  "archives, a",
					Value: "",
					Usage: "archives definitions in form: rows:step[:archive name],rows:step[:name]...",
				},
			},
			Action: initDB,
		},
		{
			Name:  "put",
			Usage: "put many values into db (as args)",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "ts",
					Value: "",
					Usage: "time stamp (in sec, date, N/now/NOW)",
				},
				cli.StringFlag{
					Name:  "columns, c",
					Value: "",
					Usage: "optional destination columns number separated by comma",
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
				cli.StringFlag{
					Name:  "columns, c",
					Value: "",
					Usage: "optional columns to get",
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
					Name:  "begin, b",
					Value: "",
					Usage: "time stamp (in sec, date, N/now/NOW)",
				},
				cli.StringFlag{
					Name:  "end, e",
					Value: "now",
					Usage: "time stamp (in sec, date, N/now/NOW)",
				},
				cli.BoolFlag{
					Name:  "include_invalid",
					Usage: "include records with no data",
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
		{
			Name:    "serve",
			Aliases: []string{"s"},
			Usage:   "start service",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "address",
					Value: ":9390",
					Usage: "web server address",
				},
			},
			Action: startServer,
		},
	}
	app.Run(os.Args)
}

func getFilenameParam(c *cli.Context) (string, bool) {
	filename := c.GlobalString("filename")
	if !c.GlobalIsSet("filename") || filename == "" {
		LogError("Missing database file name (--filename)")
		return "", false
	}
	return filename, true
}
