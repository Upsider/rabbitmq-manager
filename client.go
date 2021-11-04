package rabbitmq_manager

import (
	"fmt"
	"github.com/streadway/amqp"
)

var connection *amqp.Connection

type Config struct {
	Host         string
	Port         int
	Username     string
	Password     string
	NamingPrefix string
	NamingSuffix string
}

type RabbitClient struct {
	*amqp.Connection
}

func NewClient(cfg Config) (RabbitClient, error) {
	var err error
	conn := connection
	if conn == nil {
		uri := fmt.Sprintf("amqp://%s:%s@%s:%d", cfg.Username, cfg.Password, cfg.Host, cfg.Port)
		conn, err = amqp.Dial(uri)
	}

	if err != nil {
		return RabbitClient{}, err
	}

	return RabbitClient{
		Connection: conn,
	}, nil
}
