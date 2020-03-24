package app

import (
	"fmt"
	"go-canal-es/elastic"
	"os"
	"os/signal"
	"syscall"
)

type App struct {
	es *elastic.Client
}

func Default(config_file string) (*App, error) {
	app := &App{}
	err := InitConfig(config_file)
	cfg := new(elastic.ClientConfig)
	cfg.Addr = Conf.ESAddr
	cfg.User = Conf.ESUser
	cfg.Password = Conf.ESPassword
	cfg.HTTPS = Conf.ESHttps
	app.es = elastic.NewClient(cfg)
	return app, err
}


func (this *App) Run() {
	sc := make(chan os.Signal, 1)
	//Notify函数让signal包将输入信号转发到c。如果没有列出要传递的信号，会将所有输入信号传递到c；否则只传递列出的输入信号。
	signal.Notify(sc,
		os.Kill,
		os.Interrupt,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	done := make(chan struct{}, 1)
	go func() {
		fmt.Println("do hello world")
		done <- struct{}{}
	}()
	select {
	case n := <-sc:
		LogerDefault().Infof("receive signal %v, closing", n)
	}
	fmt.Println("done")
	<-done
}

