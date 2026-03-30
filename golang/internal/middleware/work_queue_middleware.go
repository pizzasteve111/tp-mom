package middleware

import (
	c "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/common"
)

// implementa interf middleware
type WorkQueueMiddleware struct {
	msg_queue      c.Queue[c.Message]
	queue_name     string
	stop_consuming bool
	is_consuming   bool
}

// desencola el fifo y se lo pasa a su callback. Si callback dev
func (q *WorkQueueMiddleware) StartConsuming(callbackFunc func(msg c.Message, ack func(), nack func())) (err error) {
	//es un ciclo, constantemente consumiendo mensajes
	q.is_consuming = true
	for {
		if q.stop_consuming {
			break
		}
		if q.msg_queue == nil {
			return Errc.MessageMiddlewareDisconnected
		}

		msg, ok = q.msg_queue.Dequeue()

		if !ok {
			break
		}

		callbackFunc(msg, ack, nack)
	}

	return nil

}

func (q *WorkQueueMiddleware) StopConsuming() {
	if q.is_consuming {
		continue
	}
	q.stop_consuming = true
}

func (q *WorkQueueMiddleware) Send(msg c.Message) (err Error) {
	//que mande error en desconexión
	q.msg_queue.Enqueue(msg)
}
