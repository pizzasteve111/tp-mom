package middleware

import (
	c "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/common"
	a "github.com/rabbitmq/amqp091-go"
)

// lo unico que conoce es el nombre de la queue que le interesa consumir y el stream para hablar con el broker
type ExchangeMiddleware struct {
	//este mailbox es donde nos guardamos los msjs que vamos recibiendo

	Exchange   string
	Keys       []string
	Connection *a.Connection
	Channel    *a.Channel
}

func (e *ExchangeMiddleware) StartConsuming(callbackFunc func(msg c.Message, ack func(), nack func())) error {
	return nil
}

func (e *ExchangeMiddleware) StopConsuming() {}

func (e *ExchangeMiddleware) Send(message c.Message) error {
	return nil
}

func (e *ExchangeMiddleware) Close() error {
	return nil
}
