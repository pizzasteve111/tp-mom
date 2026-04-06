package middleware

import (
	c "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/common"
	ampq "github.com/rabbitmq/amqp091-go"
)

// lo unico que conoce es el nombre de la queue que le interesa consumir y el stream para hablar con el broker
type WorkQueueMiddleware struct {
	//este mailbox es donde nos guardamos los msjs que vamos recibiendo

	QueueName  string
	Connection *ampq.Connection
	Channel    *ampq.Channel
	tag        string
}

// desencola el fifo y se lo pasa a su callback. Si callback dev
func (q *WorkQueueMiddleware) StartConsuming(callbackFunc func(msg c.Message, ack func(), nack func())) error {
	tag := q.QueueName + "-consumer"
	q.tag = tag
	if q.Channel.IsClosed() {
		return ErrMessageMiddlewareDisconnected
	}

	msgs, err := q.Channel.Consume(
		q.QueueName,
		tag, // tag fijo y conocido
		false, false, false, false, nil,
	)
	if err != nil {
		return err
	}

	for d := range msgs {
		msg := c.Message{Body: string(d.Body)}
		ack := func() { d.Ack(false) }
		nack := func() { d.Nack(false, true) }
		callbackFunc(msg, ack, nack)
	}
	return nil
}

func (q *WorkQueueMiddleware) StopConsuming() error {
	//Manda el mensaje SCQ y la queue name al broker, este lo va a descatalogar como consumidores
	//broker responde con el mensaje SCQ, que se va a leer desde el start consuming y se va a cortar la escucha
	if q.Channel.IsClosed() {
		return ErrMessageMiddlewareDisconnected
	}
	q.Channel.Cancel(q.tag, false)
	return nil

}

func (q *WorkQueueMiddleware) Send(msg c.Message) error {
	//sobre la conexión que ya tiene, manda el mensaje con el header PUB
	//devuelve error si no había conexion
	if q.Channel.IsClosed() {
		return ErrMessageMiddlewareDisconnected
	}
	return q.Channel.Publish(
		"",
		q.QueueName,
		false,
		false,
		ampq.Publishing{
			Body: []byte(msg.Body),
		},
	)
}

func (q *WorkQueueMiddleware) Close() error {
	//Manda header CLS donde avisa que cierra conn y que entonces
	//broker deje de tenerlo en cuenta.
	if q.Channel.IsClosed() {
		return ErrMessageMiddlewareDisconnected
	}
	if err := q.Channel.Close(); err != nil {
		return err
	}
	return q.Connection.Close()

}
