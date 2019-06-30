package main

import (
	"fmt"
	db2 "github.com/qiffang/prom-tools/db"
	"gopkg.in/alecthomas/kingpin.v2"
	"math"
	"os"
	"path/filepath"
	"strconv"
)

func main() {
	cli                  := kingpin.New(filepath.Base(os.Args[0]), "CLI tool for tsdb")
	dumpCmd              := cli.Command("dump", "dump samples from a TSDB")
	dbPath               := dumpCmd.Arg("db path", "database path").String()
	dumpDir				 := dumpCmd.Flag("dump-dir", "dump directory").String()
	dumpMinTime          := dumpCmd.Flag("min-time", "minimum timestamp to dump").Default(strconv.FormatInt(math.MinInt64, 10)).Int64()
	dumpMaxTime          := dumpCmd.Flag("max-time", "maximum timestamp to dump").Default(strconv.FormatInt(math.MaxInt64, 10)).Int64()

	switch kingpin.MustParse(cli.Parse(os.Args[1:])) {
	case dumpCmd.FullCommand():
		db ,err := db2.Open(*dbPath, *dumpMinTime, *dumpMaxTime)
		if err != nil {
			exitWithError(err)
		}

		if err := db.Dump(*dumpDir); err != nil {
			exitWithError(err)
		}
	}
}

func exitWithError(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

