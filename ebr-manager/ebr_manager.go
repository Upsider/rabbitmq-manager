package ebr_manager

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/streadway/amqp"

	"github.com/Upsider/rabbitmq-manager"
)

type Handler interface {
	UnMarshall(body []byte) error
	Retries() int
	JobName() string
	Ttl(retryCount int) *int
}

// EBRManager -> Exponential Backoff Retry Manager
type EBRManager struct {
	rabbitmq_manager.RabbitClient
	Ch            *amqp.Channel
	cfg           rabbitmq_manager.Config
	jobsExchange  string
	retryExchange string
	handlers      map[string]struct {
		NumWorkers int
		Handler    Handler
	}
	connMutex sync.Mutex
	connError error
	chError   error
}

func NewManager(cfg rabbitmq_manager.Config) (*EBRManager, error) {
	client, err := rabbitmq_manager.NewClient(cfg)
	if err != nil {
		return &EBRManager{}, err
	}

	ch, err := client.Channel()
	if err != nil {
		return &EBRManager{}, err
	}

	return &EBRManager{
		RabbitClient: client,
		Ch:           ch,
		cfg:          cfg,
	}, nil
}

func (e *EBRManager) Setup() error {
	go e.checkChannelClose()
	if err := e.setUpExchanges(); err != nil {
		return err
	}
	return nil
}

func (e *EBRManager) RegisterQueue(name string) (rabbitmq_manager.Queue, error) {
	e.connMutex.Lock()
	defer e.connMutex.Unlock()
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
		return rabbitmq_manager.Queue{}, err
	}

	err = e.Ch.QueueBind(
		q.Name,         // queue name
		q.Name,         // routing key
		e.jobsExchange, // exchange
		false,
		nil,
	)
	if err != nil {
		return rabbitmq_manager.Queue{}, err
	}

	queue := rabbitmq_manager.Queue{
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

func (e *EBRManager) Publish(job rabbitmq_manager.Job, args ...rabbitmq_manager.Publisher) error {
	e.connMutex.Lock()
	defer e.connMutex.Unlock()

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
