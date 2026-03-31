package middleware

import (
	"bufio"
	"fmt"
	"net"
	"strings"

	c "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/common"
)

// lo unico que conoce es el nombre de la queue que le interesa consumir y el stream para hablar con el broker
type WorkQueueMiddleware struct {
	//este mailbox es donde nos guardamos los msjs que vamos recibiendo

	queue_name string
	connection net.Conn
}

func CreateWorkQueueMidd(queue_name string, connectionSettings ConnSettings) *WorkQueueMiddleware {
	//levanto mi work queue ligada a una queue que vive en el broker.
	//establezco conexión en este metodo enviando el mensaje NEW.
	//una vez tengo esa conn (connection atributo)
	//lo que hago es reutilizarla en start consuming, send etc
	addr := fmt.Sprintf("%s:%d", connectionSettings.Hostname, connectionSettings.Port)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil
	}
	//ahora creo el work queue middl
	//esto lo retorna el factory
	return &WorkQueueMiddleware{
		queue_name: queue_name,
		connection: conn,
	}

}

// desencola el fifo y se lo pasa a su callback. Si callback dev
func (q *WorkQueueMiddleware) StartConsuming(callbackFunc func(msg c.Message, ack func(), nack func())) error {
	///manda el msj SUB con su queue_name hacia el broker a través del conn
	//una vez manda el msj, se queda en loop atento a los mensajes que le envía broker

	//uso sync cond para evitar busy waits. Se queda dormido hasta que broker lo despierte
	//por que hay mensaje nuevo

	//ver que hago con el mensaje que recibo, lo printeo?
	//llamo a la callback func y si me da ack no devuelvo error

	_, err := q.connection.Write([]byte("SUB " + q.queue_name + "\n"))
	if err != nil {
		return err
	}

	go func() {
		reader := bufio.NewReader(q.connection)

		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				return
			}

			line = strings.TrimSpace(line)

			// esperamos: "MSG contenido"
			parts := strings.SplitN(line, " ", 2)
			if len(parts) < 2 {
				continue
			}

			cmd := parts[0]
			body := parts[1]

			if cmd != "MSG" {
				continue
			}

			msg := c.Message{Body: body}

			ack := func() {
				q.connection.Write([]byte("ACK\n"))
			}

			nack := func() {
				q.connection.Write([]byte("NACK\n"))
			}

			callbackFunc(msg, ack, nack)
		}
	}()

	return nil
}

func (q *WorkQueueMiddleware) StopConsuming() {
	//Manda el mensaje SCQ y la queue name al broker, este lo va a descatalogar como consumidores
	//broker responde con el mensaje SCQ, que se va a leer desde el start consuming y se va a cortar la escucha

	//REVISAR: si alguien encola el mensaje SCQ, el middl va a pensar que dejaron de consumir
	//otra es el de tener un bool consuming donde mientras sea true en startConsuming seguimos el loop.
	//revisar problemas de concurrencia con eso

	q.connection.Write([]byte("SCQ " + q.queue_name + "\n"))

}

func (q *WorkQueueMiddleware) Send(msg c.Message) error {
	//sobre la conexión que ya tiene, manda el mensaje con el header PUB
	//devuelve error si no había conexion

	_, err := q.connection.Write([]byte(
		"PUB " + q.queue_name + " " + msg.Body + "\n",
	))
	return err
}

func (q *WorkQueueMiddleware) Close() error {
	//Manda header CLS donde avisa que cierra conn y que entonces
	//broker deje de tenerlo en cuenta.

	q.connection.Write([]byte("CLS\n"))
	return q.connection.Close()

}
