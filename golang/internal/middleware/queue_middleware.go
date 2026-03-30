package middleware

//implementa interf middleware
type QueueMiddleware struct {
	msg_queue      cola
	queue_name     string
	stop_consuming bool
	is_consuming   bool
}

//desencola el fifo y se lo pasa a su callback. Si callback dev
func (q *QueueMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) (err error) {
	//es un ciclo, constantemente consumiendo mensajes
	q.is_consuming = true
	for {
		if q.stop_consuming {
			break
		}
		if q.msg_queue == nil {
			return ErrMessageMiddlewareDisconnected
		}

		msg, ok = q.msg_queue.Dequeue()

		if !ok {
			break
		}

		callbackFunc(msg, ack, nack)
	}

	return nil

}

func (q *QueueMiddleware) StopConsuming() {
	if q.is_consuming {
		continue
	}
	q.stop_consuming = true
}

func (q *QueueMiddleware) Send(msg Message) (err Error) {
	//que mande error en desconexión
	q.msg_queue.Enqueue(msg)
}
