package producer

import (
	"errors"
	"log"
	"os"
	"time"
)
import "github.com/streadway/amqp"

const (
	// every reconnect second when fail
	reconnectDelay = 5 * time.Second
	// every push message second when fail
	resendDelay = 5 * time.Second
	// push times when fail
	resendTimes = 3
)

var (
	errNotConnected  = errors.New("not connected to the producer")
	errAlreadyClosed = errors.New("already closed: not connected to the producer")
)

type Producer struct {
	conn          *amqp.Connection
	channel       *amqp.Channel
	notifyClose   chan *amqp.Error
	notifyConfirm chan amqp.Confirmation
	done          chan bool
	isConnected   bool

	addr         string
	exchange     string
	exchangeType string
	queue        string
	routerKey 	string
	// true means auto create exchange and queue
	// false means passive create exchange and queue
	bindingMode bool

	logger *log.Logger
}

func New(addr, exchange, exchangeType, queue, routerKey string, bindingMode bool) *Producer {
	producer := Producer{
		addr:         addr,
		exchange:     exchange,
		exchangeType: exchangeType,
		queue:        queue,
		routerKey:routerKey,
		bindingMode: bindingMode,
		logger:       log.New(os.Stdout, "", log.LstdFlags),
		done:         make(chan bool),
	}

	return &producer
}

// 首次連接rabbitmq，有錯即返回
// 沒有遇錯，則開啟goroutine循環檢查是否斷線
func (p *Producer) Start() error {
	if err := p.connect(); err != nil {
		return err
	}

	go p.reconnect()
	return nil
}

// 連接conn and channel，根據bindingMode連接exchange and queue
func (p *Producer) connect() (err error) {
	p.logger.Println("attempt to connect rabbitmq")
	if p.conn, err = amqp.Dial(p.addr); err != nil {
		return err
	}
	if p.channel, err = p.conn.Channel(); err != nil {
		p.conn.Close()
		return err
	}
	p.channel.Confirm(false)

	if p.bindingMode {
		if err = p.activeBinding(); err != nil {
			return err
		}
	} else {
		if err = p.passiveBinding(); err != nil {
			return err
		}
	}

	p.isConnected = true
	p.notifyClose = make(chan *amqp.Error)
	p.notifyConfirm = make(chan amqp.Confirmation)
	p.channel.NotifyClose(p.notifyClose)
	p.channel.NotifyPublish(p.notifyConfirm)

	p.logger.Println("rabbitmq is connected")

	return nil
}

// 自動創建(如果有則覆蓋)exchange、queue，並綁定queue
func (p *Producer) activeBinding() (err error) {
	if err = p.channel.ExchangeDeclare(
		p.exchange,
		p.exchangeType,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		p.conn.Close()
		p.channel.Close()
		return err
	}

	if _, err = p.channel.QueueDeclare(
		p.queue,
		true,  // Durable
		false, // Delete when unused
		false, // Exclusive
		false, // No-wait
		nil,   // Arguments
	); err != nil {
		p.conn.Close()
		p.channel.Close()
		return err
	}
	if err = p.channel.QueueBind(
		p.queue,
		p.routerKey,
		p.exchange,
		false,
		nil,
	); err != nil {
		p.conn.Close()
		p.channel.Close()
		return err
	}

	return nil
}

// 檢查exchange及queue是否存在，若不存在，則直接返回錯誤
// 存在則綁定exchange及queue
func (p *Producer) passiveBinding() (err error) {
	if err = p.channel.ExchangeDeclarePassive(
		p.exchange,
		p.exchangeType,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		p.conn.Close()
		return err
	}

	if _, err = p.channel.QueueDeclarePassive(
		p.queue,
		true,  // Durable
		false, // Delete when unused
		false, // Exclusive
		false, // No-wait
		nil,   // Arguments
	); err != nil {
		p.conn.Close()
		return err
	}
	if err = p.channel.QueueBind(
		p.queue,
		p.routerKey,
		p.exchange,
		false,
		nil,
	); err != nil {
		p.conn.Close()
		p.channel.Close()
		return err
	}

	return nil
}

// 重新連接rabbitmq
func (p *Producer) reconnect() {
	for {
		select {
		case <- p.done:
			return
		case <- p.notifyClose:

		}

		p.isConnected = false
		for !p.isConnected {
			if err := p.connect(); err != nil {
				p.logger.Println("failed to connect rabbitmq. Retrying...")
				time.Sleep(reconnectDelay)
			}
		}
	}
}

// 當push失敗，則wait resend time，在重新push
// 累積resendTimes則返回error
// push成功，檢查有無回傳confirm，沒有也代表push失敗，並wait resend time，在重新push
func (p *Producer) Push(data []byte) error {
	if !p.isConnected {
		p.logger.Println(errNotConnected.Error())
	}
	var currentTimes int
	for {
		if err := p.channel.Publish(
			p.exchange,     // Exchange
			p.routerKey, // Routing key
			false,  // Mandatory
			false,  // Immediate
			amqp.Publishing {
				DeliveryMode: 2,
				ContentType:  "application/json",
				Body:         data,
				Timestamp:    time.Now(),
			},
		); err != nil {
			p.logger.Println("push failed. retrying...")
			currentTimes += 1
			if currentTimes < resendTimes {
				time.Sleep(reconnectDelay)
				continue
			} else {
				return err
			}
		}

		ticker := time.NewTicker(resendDelay)
		select {
		case confirm := <- p.notifyConfirm:
			if confirm.Ack {
				p.logger.Println("push confirmed!")
				return nil
			}
		case <- ticker.C:
		}
		p.logger.Println("push didn't confirm. retrying...")
	}
}

// 關閉rabbitmq conn and channel
func (p *Producer) Close() error {
	if !p.isConnected {
		return errAlreadyClosed
	}
	err := p.channel.Close()
	if err != nil {
		return err
	}
	err = p.conn.Close()
	if err != nil {
		return err
	}
	close(p.done)
	p.isConnected = false
	return nil
}
