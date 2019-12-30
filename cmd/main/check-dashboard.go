package main

import (
	"errors"
	"fmt"
	"github.com/grafana-tools/sdk"
	"github.com/qiffang/prom-tools/client"
	"github.com/qiffang/prom-tools/provider"
	"github.com/wushilin/stream"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"runtime/pprof"
	"strconv"
	"strings"
	"time"
)

const (
	grafana_url  = "http://172.16.5.155:30976"
	grafana_user = "admin"
	grafana_pwd  = "admin"
)

var (
	checkDashboards = []string{"tidb", "tikv", "pd", "overview"}
	labelReg        = regexp.MustCompile(`\(.*,`)
)

func main() {

	f, err := os.Create("cpu")
	checkError("create pprofile failed", err)
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	reg := regexp.MustCompile("(?U)\"expr\":\"(.*)\",\"")

	c := sdk.NewClient(grafana_url, fmt.Sprintf("%s:%s", grafana_user, grafana_pwd), sdk.DefaultHTTPClient)

	boardmetas, err := c.SearchDashboards("", false)
	if err != nil {
		panic(fmt.Sprintf("get board metas failed %v", err))
	}

	p, err := NewMetricsProvider(grafana_url)
	if err != nil {
		panic(fmt.Sprintf("create metrics provider failed, %v", err))
	}

	datasources, err := c.GetAllDatasources()
	if err != nil {
		panic(fmt.Sprintf("get datasources failed, %v", err))
	}
	if len(datasources) == 0 {
		checkError("", errors.New("empty datasources"))
	}

	stream.FromArray(boardmetas).Filter(func(meta sdk.FoundBoard) bool {
		if strings.Contains(strings.ToLower(meta.Title), "kafka") {
			return false
		}

		for _, t := range checkDashboards {
			if strings.Contains(strings.ToLower(meta.Title), t) {
				return true
			}
		}

		return false
	}).Each(func(meta sdk.FoundBoard) {
		fmt.Println("check dashboard: " + meta.Title)
		d, _, err := c.GetRawDashboard(meta.URI)
		if err != nil {
			panic(fmt.Sprintf("get board failed, url=%s, err=%v", meta.URI, err))
		}

		expr := reg.FindAllString(string(d), -1)

		for _, e := range expr {
			promsql := processSpecialChar(e)

			vars := getVariables(p, c, meta.URI, datasources)

			if len(vars) > 0 {
				checkWithVar(datasources, p, promsql, vars)
			} else {
				check(datasources, p, promsql, true)
			}

		}

		//panelLen := len(d.Panels)
		//
		//panelCount := 0
		//for _, r := range d.Rows {
		//	panelCount += len(r.Panels)
		//}
		//
		//if panelLen != panelCount {
		//	panic(fmt.Sprintf("pannel count wrong expect=%d, actual=%d", panelLen, panelCount))
		//}
		//
		//for _, p := range d.Panels {
		//
		//	for _, pp := range p.Panels {
		//		pp.pa
		//	}
		//}
	})
}

func processSpecialChar(sql string) string {
	promsql := strings.ReplaceAll(strings.ReplaceAll(sql, "\"expr\":", ""), ",\"", "")

	promsql, err := strconv.Unquote(promsql)
	checkError("unquote failed, sql="+sql, err)
	return promsql
}

type grafanaVar struct {
	labels map[string]string
}

func getVariables(p *provider.PrometheusProvider, c *sdk.Client, uri string, datasources []sdk.Datasource) []*grafanaVar {
	b, _, err := c.GetDashboard(uri)
	checkError("get dashboard failed", err)

	//glables := make([]*grafanaLables, len(b.Templating.List))

	list := make([]sdk.TemplateVar, 0)
	for _, tpv := range b.Templating.List {
		if tpv.Hide != 0 {
			continue
		}

		list = addPriorityList(list, tpv)
	}

	if len(list) == 0 {
		return make([]*grafanaVar, 0)
	}

	firstVar := list[0]
	if strings.Contains(firstVar.Query, "$") {
		checkError("", errors.New("first label contain $"))
	}

	values := series(p, firstVar.Query, firstVar.Name, getDatasourceId(c, firstVar.Datasource))
	grafanaVars := make([]*grafanaVar, 0)

	otherVars := list[1:]
	for _, v := range values {
		m := make(map[string]string)
		m[firstVar.Name] = v

		for _, otherVar := range otherVars {
			name, query := replace(m, otherVar)

			otherVarLable := series(p, query, name, getDatasourceId(c, otherVar.Datasource))
			if len(otherVarLable) > 0 {
				m[otherVar.Name] = otherVarLable[0]
			}
		}

		grafanaVars = append(grafanaVars, &grafanaVar{labels: m})
	}

	return grafanaVars
}

func getDatasourceId(c *sdk.Client, datasource *string) uint {
	ds, err := c.GetDatasourceByName(*datasource)
	checkError("get datasource failed", err)

	return ds.ID
}

func addPriorityList(list []sdk.TemplateVar, tmpVar sdk.TemplateVar) []sdk.TemplateVar {
	if len(list) == 0 {
		return append(list, tmpVar)
	}

	newList := make([]sdk.TemplateVar, len(list)+1)
	skip := 0
	for ; skip < len(list); skip++ {
		v := list[skip]
		if strings.Count(v.Query, "$") > strings.Count(tmpVar.Query, "$") {
			newList[skip] = tmpVar
			break
		}

		newList[skip] = v
	}

	if skip >= len(list) {
		newList[skip] = tmpVar
		return newList
	}

	for skip = skip; skip < len(list); skip++ {
		newList[skip] = list[skip]
	}

	return newList
}

func replace(labels map[string]string, templateVar sdk.TemplateVar) (string, string) {
	query := templateVar.Query
	for k, v := range labels {
		query = strings.ReplaceAll(query, "$"+k, v)
	}

	return templateVar.Name, query
}

func series(p *provider.PrometheusProvider, promSql string, name string, datasourceId uint) []string {
	now := time.Now().Unix()

	promSql = labelReg.FindAllString(promSql, -1)[0]
	promSql = promSql[1 : len(promSql)-1]

	r, err := p.Series(promSql, now-7200, now, provider.MetricInfo{Metric: "series"}, datasourceId)
	checkError("get series failed", err)

	labels := make([]string, 0)
	for _, mv := range r.Items {
		if v, ok := mv.MetricLabels[name]; ok {
			labels = append(labels, v)
		}
	}

	return labels
}

func checkWithVar(datasources []sdk.Datasource, p *provider.PrometheusProvider, promsql string, vars []*grafanaVar) bool {
	for _, v := range vars {
		for lb, lv := range v.labels {
			promsql = strings.ReplaceAll(promsql, "$"+lb, lv)
		}

		if check(datasources, p, promsql, false) {
			return true
		}
	}

	fmt.Println("WARN: " + promsql)

	return false
}

func check(datasources []sdk.Datasource, p *provider.PrometheusProvider, promsql string, needPrint bool) bool {
	for _, ds := range datasources {
		now := time.Now().Unix()
		list, err := p.GetMetricSeriesByGrafana(promsql, now-7200, now, ds.ID, provider.MetricInfo{StepInSec: 60})
		if err != nil {
			panic(fmt.Sprintf("get metric series failed, %v", err))
		}

		if len(list.Items) > 0 {
			return true
		}
	}

	if needPrint {
		fmt.Println("WARN: " + promsql)
	}

	return false
}

func checkError(msg string, err error) {
	if err != nil {
		panic(fmt.Sprintf("msg=%s, err=%v", msg, err))
	}
}

func NewMetricsProvider(baseUrl string) (*provider.PrometheusProvider, error) {
	baseURL, err := url.Parse(baseUrl)
	if err != nil {
		return nil, err
	}

	baseURL.User = url.UserPassword(grafana_user, grafana_pwd)
	c := client.NewClient(&http.Client{}, baseURL)

	return provider.NewPrometheusProvider(c), nil
}
