package main

import (
	"fmt"
	"regexp"
)

func main() {
	r := regexp.MustCompile(`\(.*,`)
	expr := r.FindAllString("label_values(pd_cluster_status, instance)", -1)
	for _, e := range expr {
		//if i == 0 {
		//	continue
		//}
		fmt.Println(e)
	}
}
