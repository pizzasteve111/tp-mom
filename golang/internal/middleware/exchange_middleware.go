package middleware

import (
	c "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/common"
	a "github.com/rabbitmq/amqp091-go"
)

// lo unico que conoce es el nombre de la queue que le interesa consumir y el stream para hablar con el broker
type ExchangeMiddleware struct {
	//tiene un exchange al que se suscriben las queues
	Exchange string
	//keys sería el arreglo de nombres de colas asociadas
	Keys []string
	//el consumer crea su propia queue donde recibe los mensajes de sus bindings
	QueueName  string
	Connection *a.Connection
	Channel    *a.Channel
	//es para identificar al consumer
	//util para los close donde mejor especificamos quien cierra conn
	consumerTag string
}

// empiezo a escuchar lo que enrutea el exchange hacia mi queue
// el mensaje lo paso a la callback
func (e *ExchangeMiddleware) StartConsuming(callbackFunc func(msg c.Message, ack func(), nack func())) error {
	tag := ""
	msgs, err := e.Channel.Consume(
		e.QueueName,
		tag,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}
	//ahora se puede identificar
	e.consumerTag = tag

	go func() {
		for d := range msgs {
			msg := c.Message{Body: string(d.Body)}

			ack := func() { d.Ack(false) }
			nack := func() { d.Nack(false, true) }

			callbackFunc(msg, ack, nack)
		}
	}()

	return nil
}

func (e *ExchangeMiddleware) StopConsuming() {
	e.Channel.Cancel(e.consumerTag, false)
}

// no mando mensaje a una queue, sino que pusheo a un exchange que luego lo
// distribuye a las keys asociadas
func (e *ExchangeMiddleware) Send(message c.Message) error {
	for _, key := range e.Keys {
		err := e.Channel.Publish(
			e.Exchange,
			key,
			false,
			false,
			a.Publishing{
				Body: []byte(message.Body),
			},
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *ExchangeMiddleware) Close() error {
	if err := e.Channel.Close(); err != nil {
		return err
	}
	return e.Connection.Close()
}
