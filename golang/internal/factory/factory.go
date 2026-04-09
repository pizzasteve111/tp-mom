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
	//hago que la cola sea persistente ante restarts
	_, err = ch.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}
	//pongo prefetch en 1, aseguro que no haya mas mensajes de los que puedo procesar
	if err = ch.Qos(1, 0, false); err != nil {
		ch.Close()
		conn.Close()
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

// instancio mi exchange, creo una queue propia y la bindeo a las N keys
func CreateExchangeMiddleware(exchange string, keys []string, settings m.ConnSettings) (m.Middleware, error) {
	conn, ch, err := connect(settings)
	if err != nil {
		return nil, err
	}

	err = ch.ExchangeDeclare(
		exchange,
		"direct",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	q, err := ch.QueueDeclare(
		"",
		false,
		true,
		true,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}
	if err = ch.Qos(1, 0, false); err != nil {
		ch.Close()
		conn.Close()
		return nil, err
	}
	for _, key := range keys {
		err = ch.QueueBind(
			q.Name,
			key,
			exchange,
			false,
			nil,
		)
		if err != nil {
			return nil, err
		}
	}

	return &m.ExchangeMiddleware{
		Exchange:   exchange,
		Keys:       keys,
		Connection: conn,
		Channel:    ch,
		QueueName:  q.Name,
	}, nil
}
