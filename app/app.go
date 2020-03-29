package app

import (
	"context"
	"fmt"
	"github.com/juju/errors"
	"go-canal-es/app/config"
	"go-canal-es/app/elastic"
	"go-canal-es/app/kafka"
	"go-canal-es/app/loger"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

type App struct {
	es *elastic.Client
	wg *sync.WaitGroup

	ctx    context.Context
	cancel context.CancelFunc
}

func Default(config_file string) (*App, error) {
	app := &App{}
	app.ctx, app.cancel = context.WithCancel(context.Background())
	app.wg = &sync.WaitGroup{}
	err := config.InitConfig(config_file)
	if err != nil {
		panic(errors.ErrorStack(err))
	}
	loger.InitLoger()

	cfg := new(elastic.ClientConfig)
	cfg.Addr = config.Conf.ESAddr
	cfg.User = config.Conf.ESUser
	cfg.Password = config.Conf.ESPassword
	cfg.HTTPS = config.Conf.ESHttps
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

	this.wg.Add(1)
	go func() {
		defer this.wg.Done()
		this.start()
	}()
	select {
	case n := <-sc:
		loger.Loger.Infof("receive signal %v, closing", n)

	case <-this.ctx.Done():
		loger.Loger.Infof("context is done with %v, closing", this.ctx.Err())
	}
	fmt.Println("close")
	this.close()
}

func (this *App) start() {
	this.wg.Add(1)
	done := make(chan struct{} ,1)
	go this.consumer(done)
	<-done
}

func (this *App) consumer(done chan struct{}) {
	defer this.wg.Done()
	defer func() {
		done<- struct{}{}
	}()
	consumer := kafka.KafkaClient{}
	consumer.Consumer(this.ctx, this.wg)
}

func (this *App) close() {
	this.cancel()
	this.wg.Wait()
}
