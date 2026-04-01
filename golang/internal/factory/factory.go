package factory

import (
	a "github.com/rabbitmq/amqp091-go"

	"fmt"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
)

func CreateQueueMiddleware(queueName string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	conn, ch, err := connect(connectionSettings)
	if err != nil {
		return nil, err
	}

	// declarar la cola (si no existe la crea)
	_, err = ch.QueueDeclare(
		queueName,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	return &m.WorkQueueMiddleware{
		QueueName:  queueName,
		Connection: conn,
		Channel:    ch,
	}, nil
}
func connect(settings m.ConnSettings) (*a.Connection, *a.Channel, error) {
	url := fmt.Sprintf("amqp://guest:guest@%s:%d/", settings.Hostname, settings.Port)

	conn, err := a.Dial(url)
	if err != nil {
		return nil, nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, nil, err
	}

	return conn, ch, nil
}
func CreateExchangeMiddleware(exchange string, keys []string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	conn, ch, err := connect(connectionSettings)
	if err != nil {
		return nil, err
	}

	return &m.ExchangeMiddleware{
		Exchange:   exchange,
		Keys:       keys,
		Connection: conn,
		Channel:    ch,
	}, nil
}
