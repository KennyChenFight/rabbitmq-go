package consumer

import (
	"errors"
	"github.com/streadway/amqp"
	"log"
	"os"
	"time"
)

const (
	// every reconnect second when fail
	reconnectDelay = 5 * time.Second
)

var (
	errAlreadyClosed = errors.New("already closed: not connected to the consumer")
)

type Consumer struct {
	conn          *amqp.Connection
	channel       *amqp.Channel
	notifyClose   chan *amqp.Error
	done          chan bool
	isConnected   bool
	isConsume 	  bool

	addr         string
	consumerTag  string
	exchange     string
	exchangeType string
	queue        string
	routerKey 	 string
	// consumer handler
	handler 	 func([]byte) error
	// true means auto create exchange and queue
	// false means passive create exchange and queue
	bindingMode bool

	logger *log.Logger
}

func New(addr, consumerTag, exchange, exchangeType, queue, routerKey string, handler func([]byte) error, bindingMode bool) *Consumer {
	consumer := Consumer{
		addr:         addr,
		consumerTag: consumerTag,
		exchange:     exchange,
		exchangeType: exchangeType,
		queue:        queue,
		routerKey:routerKey,
		handler: handler,
		bindingMode: bindingMode,
		logger:       log.New(os.Stdout, "", log.LstdFlags),
		done:         make(chan bool),
	}

	return &consumer
}

// 首次連接rabbitmq，有錯即返回
// 沒有遇錯，則開啟goroutine循環檢查是否斷線
func (c *Consumer) Start() error {
	if err := c.connect(); err != nil {
		return err
	}

	go c.reconnect()
	return nil
}

// 連接conn and channel，根據bindingMode連接exchange and queue
func (c *Consumer) connect() (err error) {
	c.logger.Println("attempt to connect rabbitmq")
	if c.conn, err = amqp.Dial(c.addr); err != nil {
		return err
	}
	if c.channel, err = c.conn.Channel(); err != nil {
		c.conn.Close()
		return err
	}

	if c.bindingMode {
		if err = c.activeBinding(); err != nil {
			return err
		}
	} else {
		if err = c.passiveBinding(); err != nil {
			return err
		}
	}

	c.isConnected = true
	c.notifyClose = make(chan *amqp.Error)
	c.channel.NotifyClose(c.notifyClose)
	c.logger.Println("rabbitmq is connected")

	return nil
}

// 自動創建(如果有則覆蓋)exchange、queue，並綁定queue
func (c *Consumer) activeBinding() (err error) {
	if err = c.channel.ExchangeDeclare(
		c.exchange,
		c.exchangeType,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		c.conn.Close()
		c.channel.Close()
		return err
	}

	if _, err = c.channel.QueueDeclare(
		c.queue,
		true,  // Durable
		false, // Delete when unused
		false, // Exclusive
		false, // No-wait
		nil,   // Arguments
	); err != nil {
		c.conn.Close()
		c.channel.Close()
		return err
	}
	if err = c.channel.QueueBind(
		c.queue,
		c.routerKey,
		c.exchange,
		false,
		nil,
	); err != nil {
		c.conn.Close()
		c.channel.Close()
		return err
	}

	return nil
}

// 檢查exchange及queue是否存在，若不存在，則直接返回錯誤
// 存在則綁定exchange及queue
func (c *Consumer) passiveBinding() (err error) {
	if err = c.channel.ExchangeDeclarePassive(
		c.exchange,
		c.exchangeType,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		c.conn.Close()
		return err
	}

	if _, err = c.channel.QueueDeclarePassive(
		c.queue,
		true,  // Durable
		false, // Delete when unused
		false, // Exclusive
		false, // No-wait
		nil,   // Arguments
	); err != nil {
		c.conn.Close()
		return err
	}
	if err = c.channel.QueueBind(
		c.queue,
		c.routerKey,
		c.exchange,
		false,
		nil,
	); err != nil {
		c.conn.Close()
		c.channel.Close()
		return err
	}

	return nil
}

// 重新連接rabbitmq，並且重新consume
// 如果連接rabbitmq失敗，會一直重試直到成功
func (c *Consumer) reconnect() {
	for {
		select {
		case <- c.done:
			return
		case <- c.notifyClose:
			c.logger.Println("rabbitmq notify close!")
		}

		c.isConnected = false
		c.isConsume = false
		for {
			if !c.isConnected {
				if err := c.connect(); err != nil {
					c.logger.Println("failed to connect rabbitmq. Retrying...")
					time.Sleep(reconnectDelay)
				}
			}
			// 檢查目前連線是成功的，並開啟Consume
			// 避免連線馬上斷線的風險
			if c.isConnected && !c.isConsume {
				if err := c.Consume(); err != nil {
					c.logger.Println("failed to consume rabbitmq. Retrying...")
					time.Sleep(reconnectDelay)
				} else {
					break
				}
			}
		}
	}
}

// 開啟消費
func (c *Consumer) Consume() (err error) {
	c.logger.Println("attempt to consume rabbitmq.")
	var delivery <- chan amqp.Delivery
	if delivery, err = c.channel.Consume(
		c.queue,
		c.consumerTag,
		false,
		false,
		false,
		false,
		nil,
	); err != nil {
		c.channel.Close()
		c.conn.Close()
		return err
	}

	c.isConsume = true
	c.logger.Println("rabbitmq is consuming")
	go c.handle(delivery)
	return nil
}

// handle data and ack
func (c *Consumer) handle(delivery <- chan amqp.Delivery) {
	for d := range delivery {
		if err := c.handler(d.Body); err == nil {
			c.logger.Println("consume success!")
			d.Ack(false)
		} else {
			c.logger.Println("some consume problem for data:", err.Error())
			d.Ack(false)
		}
	}
}

// 關閉連接及通道
func (c *Consumer) Close() error {
	if !c.isConnected {
		return errAlreadyClosed
	}
	err := c.channel.Close()
	if err != nil {
		return err
	}
	err = c.conn.Close()
	if err != nil {
		return err
	}
	close(c.done)
	c.isConnected = false
	c.isConsume = false
	return nil
}
