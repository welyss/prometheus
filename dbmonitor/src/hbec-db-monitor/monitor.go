package main

import (
	_ "github.com/go-sql-driver/mysql"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"hbec-db-monitor/collectors"
	"log"
	"net/http"
	"runtime"
	"flag"
)

var numCores = flag.Int("n", 2, "number of CPU cores to use")

func main() {
	flag.Parse()
	runtime.GOMAXPROCS(*numCores)
	reg := prometheus.NewRegistry()

	// Add the standard process and Go metrics to the custom registry.
	reg.MustRegister(
		collectors.NewSQLCollector(),
	)

	http.Handle("/hbec/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	log.Fatal(http.ListenAndServe(":8080", nil))
}
