package main

import (
	"github.com/KennyChenFight/rabbitmq-go/rabbitmq/consumer"
	"github.com/KennyChenFight/rabbitmq-go/rabbitmq/producer"
	"log"
	"time"
)

func main() {
	go func() {
		p := producer.New(
			"amqp://guest:guest@localhost:5672/",
			"test",
			"direct",
			"producer",
			"key",
			false,
		)
		if err := p.Start(); err != nil {
			log.Panic(err)
		}
		for {
			if err := p.Push([]byte("hello")); err != nil {
				// error handle
			}
			// 方便看log
			time.Sleep(1 * time.Second)
		}
	}()

	go func() {
		handler := func(data []byte) error {
			return nil
		}

		c := consumer.New(
			"amqp://guest:guest@localhost:5672/",
			"consumer",
			"test",
			"direct",
			"producer",
			"key",
			handler,
			false,
		)
		if err := c.Start(); err != nil {
			log.Panic(err)
		}

		if err := c.Consume(); err != nil {
			log.Panic(err)
		}
	}()

	select {}
}
