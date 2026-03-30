Acá se genera un proceso donde vive un Broker.

El broker es quien instancia todas las colas usadas para el intercambio de mensajes.

Broker conoce todos los middlewares y es el encargado de insertar/comunicar mensajes.

Los midd establecen conexion y pueden operar como consumers o producers. 

Work Balancing: Nuestro broker no broadcastea los mensajes a todos los consumers, elige secuencialmente a un consumer y le da el mensaje a ese mismo.

Work Queue: el producer le dice exactamente a qué cola comunicarse.

Exchange: Hay topicos que agrupan ciertas colas, el producer pushea al topico sin saber la existencia de las colas.

