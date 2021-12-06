package ebr_manager

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/streadway/amqp"

	rabbitmq_manager "github.com/Upsider/rabbitmq-manager"
)

func (e *EBRManager) work(handler Handler) {
	//retry infinitely in case channel close or something bad happens
	for {
		fmt.Println("worker start")
		chann, err := e.RabbitClient.Channel()
		if err != nil {
			fmt.Println(err)
			time.Sleep(30 * time.Second)
			continue
		}

		msgs, err := chann.Consume(
			e.resolveNaming(handler.JobName()),
			"",
			false,
			false,
			false,
			false,
			nil,
		)
		if err != nil {
			fmt.Println(err)
			time.Sleep(30 * time.Second)
			continue
		}

		err = chann.Qos(1, 0, false)
		if err != nil {
			fmt.Println(err)
			time.Sleep(30 * time.Second)
			continue
		}

		for m := range msgs {
			err := handler.UnMarshall(m.Body)
			if err != nil {
				if rErr := e.retry(m, handler); rErr != nil {
					continue
				}
			}

			m.Ack(false)
		}

		chann.Close()
	}
}

func (e *EBRManager) retry(d amqp.Delivery, handler Handler) error {
	e.connMutex.Lock()
	defer e.connMutex.Unlock()

	//determine the new ttl based on th retry count with exponential backoff
	timeStep := 10000 // in milliseconds
	retryCount := e.getXDeathCount(d)
	intTtl := int(math.Pow(2, float64(retryCount)))
	ttl := intTtl * timeStep

	//get the max retries specified by the handler
	if handler.Retries() != -1 && handler.Retries() <= retryCount {
		return e.toDead(d, handler)
	}

	//change the ttl if handler specify a custom one
	customTtl := handler.Ttl(retryCount)
	if customTtl != nil {
		ttl = *customTtl
	}

	//declare the temporary retry queue
	retryQueueName := fmt.Sprintf("%s.retry.%d", e.resolveNaming(handler.JobName()), ttl)
	rq, err := e.Ch.QueueDeclare(
		retryQueueName, // name
		true,           // durable
		false,          // delete when unused
		false,          // exclusive
		false,          // no-wait
		map[string]interface{}{
			"x-dead-letter-exchange":    e.jobsExchange,
			"x-dead-letter-routing-key": e.resolveNaming(handler.JobName()),
			"x-expires":                 ttl + 60000,
		},
	)
	if err != nil {
		return err
	}

	return e.Ch.Publish(
		"",
		rq.Name,
		false,
		false,
		amqp.Publishing{
			Headers:      d.Headers,
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/json",
			Body:         d.Body,
			Expiration:   strconv.Itoa(ttl),
		},
	)
}

func (e *EBRManager) toDead(d amqp.Delivery, handler Handler) error {
	e.connMutex.Lock()
	defer e.connMutex.Unlock()

	deadQueueName := fmt.Sprintf("%s.dead", e.resolveNaming(handler.JobName()))
	dq, err := e.Ch.QueueDeclare(
		deadQueueName, // name
		true,          // durable
		false,         // delete when unused
		false,         // exclusive
		false,         // no-wait
		nil,
	)
	if err != nil {
		return err
	}

	return e.Ch.Publish(
		"",
		dq.Name,
		false,
		false,
		amqp.Publishing{
			Headers:      d.Headers,
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/json",
			Body:         d.Body,
		},
	)
}

func (e *EBRManager) getXDeathCount(d amqp.Delivery) int {
	xDeathHeader := d.Headers["x-death"]

	jd, err := json.Marshal(xDeathHeader)
	if err != nil {
		return 0
	}

	xDeathMap := []map[string]interface{}{}
	err = json.Unmarshal(jd, &xDeathMap)
	if err != nil {
		return 0
	}

	var totalCount int
	for _, val := range xDeathMap {
		count, ok := val["count"].(float64)
		if !ok {
			continue
		}

		totalCount += int(count)
	}

	return totalCount
}

func (e *EBRManager) setUpExchanges() error {
	e.connMutex.Lock()
	defer e.connMutex.Unlock()

	e.jobsExchange = e.resolveNaming("jobs_exchange")
	if err := e.Ch.ExchangeDeclare(
		e.jobsExchange, // name
		"direct",       // type
		true,           // durable
		false,          // auto-deleted
		false,          // internal
		false,          // no-wait
		nil,            // arguments
	); err != nil {
		return err
	}

	return nil
}

func (e *EBRManager) resolveNaming(str string) string {
	if e.cfg.NamingPrefix != "" {
		str = fmt.Sprintf("%s-%s", e.cfg.NamingPrefix, str)
	}

	if e.cfg.NamingSuffix != "" {
		str = fmt.Sprintf("%s-%s", str, e.cfg.NamingSuffix)
	}

	return str
}

func (e *EBRManager) refreshConnection() {
	e.connMutex.Lock()
	defer e.connMutex.Unlock()
	var err error
	for i := 0; i < 10; i++ {
		time.Sleep(2 * time.Second)
		fmt.Printf("reconnection %d times\n", i+1)

		if e.RabbitClient.IsClosed() {
			e.RabbitClient, err = rabbitmq_manager.NewClient(e.cfg)
			if err != nil {
				fmt.Println(err)
				continue
			}
		}

		e.Ch, err = e.RabbitClient.Channel()
		if err != nil {
			fmt.Println(err)
			continue
		}

		break
	}

	if err != nil {
		panic(fmt.Sprintf("can't connect to ra, %v", err))
	} else {
		fmt.Println("connection acquired")
		go e.checkChannelClose()
	}
}

func (e *EBRManager) checkChannelClose() {
	<-e.Ch.NotifyClose(make(chan *amqp.Error))
	e.refreshConnection()
}
