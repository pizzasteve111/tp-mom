Pensamos un middleware que usa colas para el envío de mensajes.

Luego cuando hagamos el exchange lo que hacemos es aplicar una logica de routing sobre ese envío de mensajes.

Consumidor declara una cola y hace start consuming en esa para recibir sus mensajes.

Producer conoce la cola y envía mensajes por ella.

Tengo la interfaz que lleva la logica tanto de consumer como de Producer.

type ConnSettings struct {
	Hostname string
	Port     int
} esto es el setting que tiene que usar un consumer que declare.

Declaro la clase Queue Middleware que implementa la interfaz.

Este struct sería algo que tiene un Hash de colas (para cada nombre una cola única)

El método send encolaría algo dentro de hash, start consuming te hace escuchar esa cola del hash