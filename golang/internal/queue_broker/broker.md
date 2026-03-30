Acá se tiene un struct broker en el que se almacenan todas las queues en un hash de la forma <nombre><cola>.

Tambien tiene un hash active_consumers donde para cada nombre de cola, tiene un arreglo con todas las conexiones que escuchan sobre cada una.

En working queue, el producer dice exactamente a que cola enviar el mensaje. Directamente desde el middleware "encola" el mensaje donde quiere. Solo uno de los N consumers de esa cola se queda con el mensaje, esto para tareas como procesar chunks o paralelismo, a cada uno le llega un task de los N que hay

En un exchange, el producer manda el mensaje a un topico y es un Exchange middleware quien va a enrutar y enviar el mensaje a todas las colas coincidentes.

cuando un middleware hace start_consuming, se establece conexion hacia el broker para una cola en particular.
La cola va desencolando y lo manda por los sockets que ya tenía conectados en ese momento.

