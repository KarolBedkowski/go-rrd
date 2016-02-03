package main

import (
	"os"

	"github.com/codegangsta/cli"
)

// AppVersion is application version
var AppVersion = "dev"

func main() {
	app := cli.NewApp()
	app.Name = "boom"
	app.Usage = "RRD CLI tool"
	app.Version = "0.0.1 (build:" + AppVersion + ")"
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
		cli.IntFlag{
			Name:  "debug-level, D",
			Usage: "debug level (0-2)",
		},
		cli.BoolFlag{
			Name:  "no-rt",
			Usage: "TS is not real time",
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
					Name:  "include-invalid",
					Usage: "include records with no data",
				},
				cli.BoolFlag{
					Name:  "separate-valid-groups",
					Usage: "put blank line instead of invalid row (for non-continuous gnuplot graphs)",
				},
				cli.StringFlag{
					Name:  "columns, c",
					Value: "",
					Usage: "optional columns to retrieve",
				},
			},
			Action: getRangeValues,
		},
		{
			Name:   "info",
			Usage:  "show informations about rrdfile",
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
		{
			Name:  "dump",
			Usage: "dump data to external file",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "output",
					Value: "",
					Usage: "output file name",
				},
			},
			Action: dumpData,
		},
		{
			Name:  "load",
			Usage: "create rrd from dumped data",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "input",
					Value: "",
					Usage: "input file name",
				},
			},
			Action: loadData,
		},
		{
			Name:  "add-columns",
			Usage: "add new columns to rrd file",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "columns, c",
					Value: "",
					Usage: "columns definition in form: function[:col name],function[:col name],.... Functions: average/avg/sum/min/minimum/max/maximum/count/last",
				},
			},
			Action: modifyAddColumns,
		},
		{
			Name:  "del-columns",
			Usage: "remove columns from rrd file",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "columns, c",
					Value: "",
					Usage: "column indexes",
				},
			},
			Action: modifyDelColumns,
		},
		{
			Name:  "add-archives",
			Usage: "add new, empty archives to rrd file",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "archives, a",
					Value: "",
					Usage: "archives definitions in form: rows:step[:archive name],rows:step[:name]...",
				},
			},
			Action: modifyAddArchives,
		},
		{
			Name:  "del-archives",
			Usage: "remove archives from rrd file",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "archives, a",
					Value: "",
					Usage: "archive indexes",
				},
			},
			Action: modifyDelArchives,
		},
		{
			Name:  "resize-archive",
			Usage: "change number of rows in archive",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "archive, a",
					Value: "",
					Usage: "archive to change",
				},
				cli.IntFlag{
					Name:  "rows, r",
					Usage: "number of rows",
				},
			},
			Action: modifyResizeArchive,
		},
		{
			Name:  "gen-random",
			Usage: "fill archive with random data",
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
				cli.IntFlag{
					Name:  "step",
					Value: 1,
					Usage: "step [s]",
				},
			},
			Action: genRandomData,
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
