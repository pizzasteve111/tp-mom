package queue_broker

//acá es donde se levanta el broker
//el mismo se mantiene escuchando conexiones entrantes

//Un middleware establece conexion mandando un mensaje.

//El mensaje puede tener el header PUB si quiere enviar un mensaje a una cola, o SUB si quiere consumir una cola.

//dependiendo del header, broker llama a sus metodos handlers.
import (
	"bufio"
	"fmt"
	"net"
	"strings"

	c "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/common"
)

func main() {
	broker := NewBroker()
	//revisar puerto
	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		panic(err)
	}
	defer listener.Close()
	fmt.Println("Broker escuchando en :8080")
	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}

		go handleConnection(conn, broker)
	}
}

func handleConnection(conn net.Conn, broker *Broker) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}

		line = strings.TrimSpace(line)
		parts := strings.Split(line, " ")

		if len(parts) == 0 {
			continue
		}

		cmd := parts[0]

		switch cmd {

		// SUB queueName
		case "SUB":
			if len(parts) < 2 {
				continue
			}
			queueName := parts[1]

			broker.HandleConsuming(queueName, conn)

		// PUB queueName message...
		case "PUB":
			if len(parts) < 3 {
				continue
			}

			queueName := parts[1]
			msgBody := strings.Join(parts[2:], " ")

			msg := c.Message{Body: msgBody}

			broker.HandleMsg(queueName, msg)
		//Close, cierro conexion total con broker,
		case "CLS":
			//terminar
			return
		//stop consuming queue: le indico al broker que me saque del hash
		case "SCQ":
			queueName := parts[1]
			broker.HandleStopConsuming(queueName, conn)
		}

	}
}
