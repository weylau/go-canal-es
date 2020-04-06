package app

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/juju/errors"
	"go-canal-es/app/config"
	"go-canal-es/app/elastic"
	"go-canal-es/app/loger"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
)

const (
	UpdateAction = "update"
	IndexAction  = "index"//创建或添加
	InsertAction = "insert"
	DeleteAction = "delete"
)

type CanalData struct {
	Data     interface{} `json:"data"`
	Database string      `json:"database"`
	Table    string      `json:"table"`
	Type     string      `json:"type"`
}

type ConsumerHandler struct {
	Ready chan bool
	app   *App
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *ConsumerHandler) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(consumer.Ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *ConsumerHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *ConsumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		canalData := CanalData{}
		json.Unmarshal(message.Value, &canalData)
		fmt.Println(canalData.Table)
		consumer.app.syncCh <- canalData
		session.MarkMessage(message, "")
	}

	return nil
}

type App struct {
	es *elastic.Client
	wg *sync.WaitGroup

	ctx    context.Context
	cancel context.CancelFunc

	syncCh chan interface{}

	rules map[string]*config.Rule

	consumerHandler *ConsumerHandler

	done chan struct{}
}

func Default(config_file string) (*App, error) {
	app := &App{}
	app.syncCh = make(chan interface{}, 4096)
	app.done = make(chan struct{})
	app.rules = make(map[string]*config.Rule)
	app.ctx, app.cancel = context.WithCancel(context.Background())
	app.wg = &sync.WaitGroup{}
	app.consumerHandler = &ConsumerHandler{
		Ready: make(chan bool),
		app:   app,
	}
	err := config.InitConfig(config_file)
	if err != nil {
		panic(errors.ErrorStack(err))
	}
	err = app.initRule()
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

func (this *App) initRule() error {

	if config.Conf.Rules != nil {
		for _, rule := range config.Conf.Rules {
			if len(rule.Database) == 0 {
				return errors.Errorf("empty database not allowed for rule")
			}
			key := ruleKey(rule.Database, rule.Table)
			rule.InitRule()
			this.rules[key] = rule
		}
	}
	return nil
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
	this.wg.Add(2)
	done := make(chan struct{}, 2)
	go this.consumer(done)
	go this.pushEs(done)
	<-done
	<-done
}

func (this *App) pushEs(done chan struct{}) {
	defer this.wg.Done()
	defer func() {
		done <- struct{}{}
	}()

	for {
		select {
		case v := <-this.syncCh:
			switch v := v.(type) {
			case CanalData:
				bulkRequest := this.CanalDataToBulkRequest(&v)

				res, err := this.es.Bulk(bulkRequest)
				if err != nil {
					this.cancel()
					loger.Loger.Error(errors.ErrorStack(errors.Trace(err)))
					return
				}
				if res.Code != 200 {
					loger.Loger.Errorf("es response code is not 200 res:%v,bulkRequest:%v", res,bulkRequest)
				}
			}
		default:
			select {
			case <-this.done:
				return
			default:

			}

		}

	}
}

func (this *App) consumer(done chan struct{}) {
	defer this.wg.Done()
	defer func() {
		done <- struct{}{}
	}()
	version, err := sarama.ParseKafkaVersion(config.Conf.KafkaVersion)
	if err != nil {
		panic(fmt.Errorf("Error parsing Kafka version: %v", err))
	}
	kfkconfig := sarama.NewConfig()
	kfkconfig.Version = version
	kfkconfig.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange

	client, err := sarama.NewConsumerGroup(strings.Split(config.Conf.ConsumerBrocks, ","), config.Conf.ConsumerGroup, kfkconfig)
	if err != nil {
		panic(fmt.Errorf("Error creating consumer group client: %v", err))
	}
	doneConsumer := make(chan struct{})
	this.wg.Add(1)
	go func() {
		defer this.wg.Done()
		defer func() {
			doneConsumer <- struct{}{}
		}()
		for {
			select {
			case <-this.ctx.Done():
				this.done <- struct{}{}
				return
			default:
				if err := client.Consume(this.ctx, strings.Split(config.Conf.ConsumerTopics, ","), this.consumerHandler); err != nil {
					panic(fmt.Errorf("Error from consumer: %v", err))
				}
				this.consumerHandler.Ready = make(chan bool)
			}
		}

	}()

	<-this.consumerHandler.Ready
	fmt.Println("Sarama consumer up and running!...")
	<-doneConsumer
	if err = client.Close(); err != nil {
		panic(fmt.Errorf("Error closing client: %v", err))
	}
}

func (this *App) close() {
	this.cancel()
	this.wg.Wait()
}

func (this *App) CanalDataToBulkRequest(canalData *CanalData) []*elastic.BulkRequest {
	canalType := strings.ToLower(canalData.Type)
	canalRuleKey := ruleKey(canalData.Database, canalData.Table)
	if rule, ok := this.rules[canalRuleKey]; ok {
		fmt.Println("rule:", rule)
		bulkReqs := make([]*elastic.BulkRequest, 0, len(canalData.Data.([]interface{})))
		switch canalType {
		case UpdateAction, InsertAction:
			for _, v := range canalData.Data.([]interface{}) {
				if id, ok := v.(map[string]interface{})[rule.ID]; ok {
					req := &elastic.BulkRequest{Index: rule.Index, Type: rule.Type, ID: id.(string), Action: IndexAction}
					req.Data = make(map[string]interface{})
					for field, value := range v.(map[string]interface{}) {
						if _, ok := rule.FieldMapping[field]; ok {
							req.Data[field] = value
						}
					}
					bulkReqs = append(bulkReqs, req)
				}

			}
		case DeleteAction:
			for _, v := range canalData.Data.([]interface{}) {
				if id, ok := v.(map[string]interface{})[rule.ID]; ok {
					req := &elastic.BulkRequest{Index: rule.Index, Type: rule.Type, ID: id.(string), Action: canalType}
					bulkReqs = append(bulkReqs, req)
				}

			}
		}
		return bulkReqs
	}

	return nil
}

func ruleKey(database string, table string) string {
	return strings.ToLower(fmt.Sprintf("%s:%s", database, table))
}
