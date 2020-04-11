package main

import (
	"flag"
	"go-canal-es/app"
	"go-canal-es/app/config"
)

var configFile = flag.String("config", "./etc/config.toml", "go-canal-es config file")
var esAddr = flag.String("es_addr", "", "Elasticsearch addr")
var logLevel = flag.String("log_level", "info", "log level")

func main() {
	flag.Parse()
	application,err := app.Default(*configFile)
	if *esAddr != "" {
		config.Conf.ESAddr = *esAddr
	}
	if *logLevel != "" {
		config.Conf.LogLevel = *logLevel
	}
	if err != nil {
		panic(error(err))
	}
	application.Run()
}
