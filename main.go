package main

import (
	"flag"
	"go-canal-es/app"
)

var configFile = flag.String("config", "./etc/config.toml", "go-canal-es config file")
var es_addr = flag.String("es_addr", "", "Elasticsearch addr")
var logLevel = flag.String("log_level", "info", "log level")

func main() {
	application,err := app.Default(*configFile)
	flag.Parse()
	if *es_addr != "" {
		app.Conf.ESAddr = *es_addr
	}
	if *logLevel != "" {
		app.Conf.LogLevel = *logLevel
	}
	if err != nil {
		panic(error(err))
	}
	application.Run()
}
