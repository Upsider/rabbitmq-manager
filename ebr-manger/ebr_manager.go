package ebr_manger

import (
	"encoding/json"
	"fmt"
	"github.com/Upsider/rabbitmq-manager"
	"github.com/streadway/amqp"
	"math"
	"strconv"
	"sync"
)

func NewManager(cfg rabbit_mq.Config) (EBRManager, error) {
	client, err := rabbit_mq.NewClient(cfg)
	if err != nil {
		return EBRManager{}, err
	}

	ch, err := client.Channel()
	if err != nil {
		return EBRManager{}, err
	}

	return EBRManager{
		RabbitClient: client,
		Ch:           ch,
		cfg:          cfg,
	}, nil
}

type Handler interface {
	UnMarshall(body []byte) error
	Retries() int
	JobName() string
	Ttl(retryCount int) *int
}

// EBRManager -> Exponential Backoff Retry Manager
type EBRManager struct {
	rabbit_mq.RabbitClient
	Ch            *amqp.Channel
	cfg           rabbit_mq.Config
	jobsExchange  string
	retryExchange string
	handlers      map[string]struct {
		NumWorkers int
		Handler    Handler
	}
}

func (e *EBRManager) Setup() error {
	if err := e.setUpExchanges(); err != nil {
		return err
	}
	return nil
}

func (e *EBRManager) RegisterQueue(name string) (rabbit_mq.Queue, error) {
	queueName := e.resolveNaming(name)
	q, err := e.Ch.QueueDeclare(
		queueName, // name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		return rabbit_mq.Queue{}, err
	}

	err = e.Ch.QueueBind(
		q.Name,         // queue name
		q.Name,         // routing key
		e.jobsExchange, // exchange
		false,
		nil,
	)
	if err != nil {
		return rabbit_mq.Queue{}, err
	}

	queue := rabbit_mq.Queue{
		Name: q.Name,
	}

	return queue, err
}

func (e *EBRManager) RegisterHandler(numWorkers int, handler Handler) {
	if e.handlers == nil {
		e.handlers = map[string]struct {
			NumWorkers int
			Handler    Handler
		}{}
	}
	e.handlers[handler.JobName()] = struct {
		NumWorkers int
		Handler    Handler
	}{
		NumWorkers: numWorkers,
		Handler:    handler,
	}
}

func (e *EBRManager) Run() {
	var wg sync.WaitGroup

	for _, val := range e.handlers {
		numWorkers := val.NumWorkers
		handler := val.Handler
		for i := 0; i < numWorkers; i++ {
			wg.Add(1)
			go func() {
				e.work(handler)
				wg.Done()
			}()
		}
	}

	wg.Wait()
}

func (e *EBRManager) Publish(job rabbit_mq.Job, args ...rabbit_mq.Publisher) error {
	body, err := json.Marshal(job.Args)
	if err != nil {
		return err
	}

	//determine the correct exchange
	exchange := e.jobsExchange
	queue := e.resolveNaming(job.JobName)
	if len(args) > 0 {
		exchange = args[0].Exchange
		queue = args[0].Queue
	}

	//create a temporary queue to hold back in case for scheduled jobs
	var expiration string
	if job.After != nil {
		expiration = fmt.Sprintf("%d", job.After.Milliseconds())
		delayQueueName := fmt.Sprintf("%s.delay.%d", queue, job.After.Milliseconds())
		delayQueue, rErr := e.Ch.QueueDeclare(
			delayQueueName, // name
			true,           // durable
			false,          // delete when unused
			false,          // exclusive
			false,          // no-wait
			map[string]interface{}{
				"x-dead-letter-exchange":    exchange,
				"x-dead-letter-routing-key": queue,
				"x-expires":                 job.After.Milliseconds() + 60000,
			},
		)
		if rErr != nil {
			return err
		}

		queue = delayQueue.Name
		exchange = ""
	}

	return e.Ch.Publish(
		exchange,
		queue,
		false,
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/json",
			Body:         body,
			Expiration:   expiration,
		},
	)
}

func (e *EBRManager) work(handler Handler) {
	msgs, err := e.Ch.Consume(
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
		return
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
}

func (e *EBRManager) retry(d amqp.Delivery, handler Handler) error {
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
			"x-expires":                 intTtl + 60000,
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
			Expiration:   strconv.Itoa(intTtl),
		},
	)
}

func (e *EBRManager) toDead(d amqp.Delivery, handler Handler) error {
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
