package queue_broker

import (
	"net"
	"sync"

	c "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/common"
)

//este es el proceso donde vive mi broker, quien persiste las distintas colas de mensajes.
//tiene un hash donde por cada nombre de cola tiene la cola asociada.

type Broker struct {
	mu     sync.Mutex
	queues map[string]*c.Queue[c.Message]
	//en este mapa, para cada nombre de cola, tiene las conexiones de los middlewares que la estan consumiendo
	active_consumers map[string][]net.Conn
	//para cada cola, me guardo el indice del ultimo consumer que le llego msj
	consumer_order map[string]int
}

func NewBroker() *Broker {
	return &Broker{
		queues:           make(map[string]*c.Queue[c.Message]),
		active_consumers: make(map[string][]net.Conn),
		consumer_order:   make(map[string]int),
	}
}

func (b *Broker) HandleStopConsuming(queue_name string, middleware_conn net.Conn) {
	//que borre el elemento de los hash, no se lo vuelva  a notificar
	b.mu.Lock()
	defer b.mu.Unlock()

	consumers := b.active_consumers[queue_name]

	newConsumers := make([]net.Conn, 0, len(consumers))

	for _, c := range consumers {
		if c != middleware_conn {
			newConsumers = append(newConsumers, c)
		}
	}

	b.active_consumers[queue_name] = newConsumers

	if len(newConsumers) == 0 {
		b.consumer_order[queue_name] = 0
	} else {
		b.consumer_order[queue_name] = b.consumer_order[queue_name] % len(newConsumers)
	}
}

func (b *Broker) HandleConsuming(queue_name string, middleware_conn net.Conn) {
	//escucha por conexiones entrantes de un middleware que quiere consumir X cola
	//lo agrega al hash
	//devuelve error si no existe la cola por ejemplo

	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.queues[queue_name]; !ok {
		b.queues[queue_name] = c.NewQueue[c.Message]()
	}

	b.active_consumers[queue_name] = append(b.active_consumers[queue_name], middleware_conn)
}

func (b *Broker) HandleMsg(queue_name string, msg c.Message) {
	//recibe un mensaje para una cola en particular, lo manda a encolar
	//obtiene la cola del hash y lo agrega.
	b.mu.Lock()
	q := b.queues[queue_name]
	q.Enqueue(msg)

	consumers := b.active_consumers[queue_name]

	if len(consumers) == 0 {
		b.mu.Unlock()
		return
	}
	//el mensaje se tiene que enviar fuera del lock, sino puede pasarme de no mandarlo por
	//no acceder al recurso

	idx := b.consumer_order[queue_name]
	conn := consumers[idx]

	b.consumer_order[queue_name] = (idx + 1) % len(consumers)

	msg, _ = q.Dequeue()

	b.mu.Unlock()

	SendMsg(conn, msg)

}

func SendMsg(conn net.Conn, msg c.Message) {
	conn.Write([]byte(msg.Body + "\n"))
}
