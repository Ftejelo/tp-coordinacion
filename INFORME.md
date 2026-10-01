# Informe

El sistema procesa varios clientes en paralelo y mantiene la identidad de cada uno durante toda la ejecución.\
Cuando un cliente se conecta al Gateway, se le asigna un `client_id` y se incluye en todos los mensajes que circulan entre Gateway, Sum, Aggregation y Joiner.\

El flujo es simple: cada cliente envía pares `(fruta, cantidad)` al Gateway; este los reenvía a las instancias de Sum mediante RabbitMQ.\
Cada instancia de Sum acumula los valores por `client_id` y por fruta.\
Cuando termina el procesamiento de un cliente, Sum emite los totales acumulados y, si hay réplicas, coordina el fin entre ellas para que todas finalicen de forma consistente.

Las instancias de Aggregation reciben esos totales y vuelven a agruparlos por `client_id`, sumando los valores de la misma fruta y calculando un top parcial para ese cliente.\
Cuando ya recibieron todas las contribuciones, envían ese top parcial al Joiner, que combina los tops de todas las réplicas y produce el resultado final para cada cliente.\
De esta forma, el sistema escala bien porque cada etapa trabaja con estado por cliente y no depende de un único acumulador global.