package middleware

import (
	"fmt"
	"sync/atomic"

	c "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/common"
	a "github.com/rabbitmq/amqp091-go"
)

var exchangeConsumerCounter uint64

// lo unico que conoce es el nombre de la queue que le interesa consumir y el stream para hablar con el broker
type ExchangeMiddleware struct {
	//tiene un exchange al que se suscriben las queues
	Exchange string
	//keys sería el arreglo de nombres de colas asociadas
	Keys []string
	//el consumer crea su propia queue donde recibe los mensajes de sus bindings
	queueName  string
	Connection *a.Connection
	Channel    *a.Channel
	//es para identificar al consumer
	//util para los close donde mejor especificamos quien cierra conn
	consumerTag string
}

// empiezo a escuchar lo que enrutea el exchange hacia mi queue
// el mensaje lo paso a la callback
func (e *ExchangeMiddleware) StartConsuming(callbackFunc func(msg c.Message, ack func(), nack func())) error {
	//error => si estaba desconectado el channel, que devuelva  ErrMessageMiddlewareDisconnected
	//un if channel esta desconected, devolvemos el error
	if e.Channel.IsClosed() {
		return ErrMessageMiddlewareDisconnected
	}
	//genero queue si no la había, es propia del struct y no persiste luego de consumirla
	q, err := e.Channel.QueueDeclare(
		"",
		false,
		true,
		true,
		false,
		nil,
	)
	if err != nil {
		return ErrMessageMiddlewareMessage
	}
	e.queueName = q.Name
	for _, key := range e.Keys {
		if err := e.Channel.QueueBind(e.queueName, key, e.Exchange, false, nil); err != nil {
			return ErrMessageMiddlewareMessage
		}
	}

	id := atomic.AddUint64(&exchangeConsumerCounter, 1)
	e.consumerTag = fmt.Sprintf(e.Exchange, id)
	//ahora se puede identificar

	msgs, err := e.Channel.Consume(
		e.queueName,
		e.consumerTag,
		false, false, false, false, nil,
	)
	if err != nil {
		return ErrMessageMiddlewareMessage
	}

	// bloqueante — el caller usa `go middleware.StartConsuming(...)`
	for d := range msgs {
		delivery := d
		msg := c.Message{Body: string(delivery.Body)}
		ack := func() { delivery.Ack(false) }
		nack := func() { delivery.Nack(false, true) }
		callbackFunc(msg, ack, nack)
	}
	return nil
}

func (e *ExchangeMiddleware) StopConsuming() error {
	if e.Channel.IsClosed() {
		return ErrMessageMiddlewareDisconnected
	}
	//if de si el channel ya esta desconectado ErrMessageMiddlewareDisconnected
	if err := e.Channel.Cancel(e.consumerTag, false); err != nil {
		return ErrMessageMiddlewareDisconnected
	}
	return nil
}

// no mando mensaje a una queue, sino que pusheo a un exchange que luego lo
// distribuye a las keys asociadas
func (e *ExchangeMiddleware) Send(message c.Message) error {
	if e.Channel.IsClosed() {
		return ErrMessageMiddlewareDisconnected
	}
	for _, key := range e.Keys {
		//si channel desconectado ErrMessageMiddlewareDisconnected
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
			return ErrMessageMiddlewareMessage
		}
	}
	return nil
}

func (e *ExchangeMiddleware) Close() error {
	if e.Channel.IsClosed() {
		return ErrMessageMiddlewareClose
	}
	if err := e.Channel.Close(); err != nil {
		return ErrMessageMiddlewareClose
	}
	if err := e.Connection.Close(); err != nil {
		return ErrMessageMiddlewareClose
	}
	return nil
}
