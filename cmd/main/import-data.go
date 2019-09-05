package main

import (
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/grafana-tools/sdk"
	"github.com/pkg/errors"
	"github.com/prometheus/common/log"
	"github.com/tidwall/sjson"
	"github.com/wushilin/stream"
	"gopkg.in/alecthomas/kingpin.v2"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	datasource = "json-import"
)

func main() {
	cli := kingpin.New(filepath.Base(os.Args[0]), "CLI tool for import prometheus query data")
	importCmd := cli.Command("import", "import Promtheus data")
	dbPath := importCmd.Arg("data path", "json directory path").String()
	grafanaUrl := importCmd.Flag("grafana-url", "grafana address").Default("http://localhost:3000").String()
	grafanaUser := importCmd.Flag("grafana-user", "grafana user").Default("admin").String()
	grafanaPwd := importCmd.Flag("grafana-pwd", "grafana password").Default("admin").String()

	switch kingpin.MustParse(cli.Parse(os.Args[1:])) {
	case importCmd.FullCommand():
		NewServer(*dbPath, *grafanaUrl, *grafanaUser, *grafanaPwd).start()
	}
}

type status string
type errorType string

type response struct {
	Status    status      `json:"status"`
	Data      interface{} `json:"data,omitempty"`
	ErrorType errorType   `json:"errorType,omitempty"`
	Error     string      `json:"error,omitempty"`
}

type server struct {
	files       map[string]string
	grafanaUrl  string
	grafanaUser string
	grafanaPwd  string
	board       *sdk.Board
}

func NewServer(path string, grafanaUrl string, grafanaUser string, grafanaPwd string) server {
	files := make(map[string]string, 0)
	filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}

		if filepath.Ext(path) == ".json" {
			files[extract(strings.TrimSuffix(path, filepath.Ext(path)))] = path
		}

		return nil
	})

	return server{
		files:       files,
		grafanaUrl:  grafanaUrl,
		grafanaUser: grafanaUser,
		grafanaPwd:  grafanaPwd,
		board:       createDashboard(),
	}
}

func (s server) start() {
	s.initGrafana()
	engine := gin.Default()

	engine.GET("/api/v1/query", s.fakeQuery)
	engine.GET("/api/v1/query_range", s.fakeQuery)

	log.Fatal("StartServer server failed", engine.Run("0.0.0.0:8080").Error())
}

func (s server) initGrafana() {
	row := s.board.AddRow("Import json format")

	stream.FromMapKeys(s.files).Map(func(query string) string {
		str, err := sjson.Set(template, "targets.0.expr", query)
		checkErr(err, "replace expr failed")

		str, err = sjson.Set(str, "title", query)
		checkErr(err, "replace title failed")

		return str
	}).Filter(func(graph string) bool {
		return graph != ""
	}).Map(func(graph string) *sdk.Panel {
		var panel sdk.Panel
		if err := json.Unmarshal([]byte(graph), &panel); err != nil {
			log.Error("Unmarshal failed", err)
			return nil
		}

		return &panel
	}).Each(func(panel *sdk.Panel) {
		row.Add(panel)
	})

	s.AddDashboard()
}

func (s server) fakeQuery(c *gin.Context) {
	queryFile, ok := s.files[c.Query("query")]
	if !ok {
		log.Error("file does not exist", "query="+c.Query("query"))
		c.JSON(http.StatusBadRequest, errors.New("file does not exist"))
		return
	}

	b, err := ioutil.ReadFile(queryFile)
	if err != nil {
		log.Error("query failed", "file="+queryFile, err)
		c.JSON(http.StatusBadRequest, err)
		return
	}

	var r response
	if err = json.Unmarshal(b, &r); err != nil {
		log.Error("read file failed", "file="+queryFile, err)
		c.JSON(http.StatusBadRequest, err)
		return
	}

	c.JSON(http.StatusOK, r)
}

func (s server) AddDashboard() {
	c := sdk.NewClient(s.grafanaUrl, fmt.Sprintf("%s:%s", s.grafanaUser, s.grafanaPwd), sdk.DefaultHTTPClient)

	if _, err := c.CreateDatasource(createDasource()); err != nil {
		checkErr(err, "error on create dasource")
	}

	if err := c.SetDashboard(*s.board, false); err != nil {
		checkErr(err, fmt.Sprintf("error on create dashboard %s", s.board.Title))
	}
}

func createDashboard() *sdk.Board {
	board := sdk.NewBoard(fmt.Sprintf("Import json format dashboard - %d", time.Now().Unix()))
	board.ID = uint(time.Now().Unix())
	board.Time = sdk.Time{
		From: "now-5d",
		To:   "now",
	}

	return board
}

func createDasource() sdk.Datasource {
	return sdk.Datasource{
		Name:   datasource,
		Type:   "prometheus",
		Access: "proxy",
		URL:    "http://127.0.0.1:8080",
	}
}

func checkErr(err error, msg string) {
	if err != nil {
		panic(errors.Wrap(err, msg))
	}
}

func extract(path string) string {
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == filepath.Separator {
			return path[i+1:]
		}
	}
	return path
}

var template = `
{
  "aliasColors": {},
  "bars": false,
  "dashLength": 10,
  "dashes": false,
  "datasource": "json-import",
  "fill": 1,
  "gridPos": {
    "h": 9,
    "w": 12,
    "x": 0,
    "y": 0
  },
  "id": 2,
  "legend": {
    "avg": false,
    "current": false,
    "max": false,
    "min": false,
    "show": true,
    "total": false,
    "values": false
  },
  "lines": true,
  "linewidth": 1,
  "nullPointMode": "null",
  "percentage": false,
  "pointradius": 2,
  "points": false,
  "renderer": "flot",
  "seriesOverrides": [],
  "spaceLength": 10,
  "stack": false,
  "steppedLine": false,
  "targets": [
    {
      "expr": "TEST-EXPRESSION",
      "format": "time_series",
      "instant": false,
      "intervalFactor": 1,
      "refId": "A"
    }
  ],
  "thresholds": [],
  "timeFrom": null,
  "timeRegions": [],
  "timeShift": null,
  "title": "Panel Title",
  "tooltip": {
    "shared": true,
    "sort": 0,
    "value_type": "individual"
  },
  "type": "graph",
  "xaxis": {
    "buckets": null,
    "mode": "time",
    "name": null,
    "show": true,
    "values": []
  },
  "yaxes": [
    {
      "format": "short",
      "label": null,
      "logBase": 1,
      "max": null,
      "min": null,
      "show": true
    },
    {
      "format": "short",
      "label": null,
      "logBase": 1,
      "max": null,
      "min": null,
      "show": true
    }
  ],
  "yaxis": {
    "align": false,
    "alignLevel": null
  }
}

`
