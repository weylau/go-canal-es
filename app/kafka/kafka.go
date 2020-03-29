package kafka

import (
	"github.com/Shopify/sarama"
	"golang.org/x/net/context"
	"strings"
	"go-canal-es/app/config"
	"sync"
)
import "fmt"

type KafkaClient struct {

}

func (kafka *KafkaClient)Consumer(ctx context.Context, wg *sync.WaitGroup) {
	version, err := sarama.ParseKafkaVersion(config.Conf.KafkaVersion)
	if err != nil {
		panic(fmt.Errorf("Error parsing Kafka version: %v", err))
	}
	kfkconfig := sarama.NewConfig()
	kfkconfig.Version = version
	kfkconfig.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange

	consumer := Consumer{
		Ready: make(chan bool),
	}
	client, err := sarama.NewConsumerGroup(strings.Split(config.Conf.ConsumerBrocks, ","), config.Conf.ConsumerGroup, kfkconfig)
	if err != nil {
		panic(fmt.Errorf("Error creating consumer group client: %v", err))
	}
	consumerDone := make(chan struct{} ,1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer func() {
			consumerDone<- struct{}{}
		}()
		for {
			select {
			case <-ctx.Done():
				fmt.Println("this.ctx.Done")
				return
			default:
			}
			// `Consume` should be called inside an infinite loop, when a
			// server-side rebalance happens, the consumer session will need to be
			// recreated to get the new claims
			if err := client.Consume(ctx, strings.Split(config.Conf.ConsumerTopics, ","), &consumer); err != nil {
				panic(fmt.Errorf("Error from consumer: %v", err))
			}
			consumer.Ready = make(chan bool)
		}

	}()

	<-consumer.Ready // Await till the consumer has been set up
	fmt.Println("Sarama consumer up and running!...")
	<-consumerDone
	if err = client.Close(); err != nil {
		panic(fmt.Errorf("Error closing client: %v", err))
	}
}


type Consumer struct {
	Ready chan bool
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(consumer.Ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
	for message := range claim.Messages() {
		fmt.Printf("Message claimed: value = %s, timestamp = %v, topic = %s", string(message.Value), message.Timestamp, message.Topic)
		session.MarkMessage(message, "")
	}

	return nil
}
