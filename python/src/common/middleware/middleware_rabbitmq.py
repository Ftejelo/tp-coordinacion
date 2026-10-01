import pika
import logging
from .middleware import (
    MessageMiddlewareQueue,
    MessageMiddlewareExchange,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareMessageError,
    MessageMiddlewareCloseError
)

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self.host = host
        self.queue_name = queue_name
        self.connection = None
        self.channel = None
        self._consuming = False
        self._connect()

    def _connect(self):
        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue=self.queue_name, durable=True)
        except Exception as e:
            raise MessageMiddlewareDisconnectedError(f"Failed to connect to RabbitMQ: {e}")

    def start_consuming(self, on_message_callback):
        self._consuming = True
        def _callback(ch, method, properties, body):
            def ack():
                ch.basic_ack(delivery_tag=method.delivery_tag)
            def nack():
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            try:
                on_message_callback(body, ack, nack)
            except Exception as e:
                logging.error(f"Error processing message: {e}")
                nack()

        try:
            self.channel.basic_qos(prefetch_count=1)
            self.channel.basic_consume(queue=self.queue_name, on_message_callback=_callback)
            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError("Lost connection to RabbitMQ")
        except Exception as e:
            raise MessageMiddlewareMessageError(f"Error consuming messages: {e}")

    def stop_consuming(self):
        if self._consuming and self.channel:
            self.channel.stop_consuming()
            self._consuming = False

    def send(self, message):
        try:
            self.channel.basic_publish(
                exchange='',
                routing_key=self.queue_name,
                body=message,
                properties=pika.BasicProperties(delivery_mode=2)
            )
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError("Lost connection to RabbitMQ")
        except Exception as e:
            raise MessageMiddlewareMessageError(f"Error sending message: {e}")

    def close(self):
        self.stop_consuming()
        if self.connection and not self.connection.is_closed:
            try:
                self.connection.close()
            except Exception as e:
                raise MessageMiddlewareCloseError(f"Error closing connection: {e}")


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        self.host = host
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys
        self.connection = None
        self.channel = None
        self._connect()

    def _connect(self):
        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
            self.channel = self.connection.channel()
            self.channel.exchange_declare(exchange=self.exchange_name, exchange_type='direct', durable=True)
        except Exception as e:
            raise MessageMiddlewareDisconnectedError(f"Failed to connect to RabbitMQ: {e}")

    def start_consuming(self, on_message_callback):
        queue_name = f"{self.exchange_name}_{''.join(self.routing_keys)}"
        self.channel.queue_declare(queue=queue_name, durable=True, exclusive=False)
        for routing_key in self.routing_keys:
            self.channel.queue_bind(queue=queue_name, exchange=self.exchange_name, routing_key=routing_key)
        
        self._consuming = True
        def _callback(ch, method, properties, body):
            def ack():
                ch.basic_ack(delivery_tag=method.delivery_tag)
            def nack():
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            try:
                on_message_callback(body, ack, nack)
            except Exception as e:
                logging.error(f"Error processing message: {e}")
                nack()

        try:
            self.channel.basic_qos(prefetch_count=1)
            self.channel.basic_consume(queue=queue_name, on_message_callback=_callback)
            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError("Lost connection to RabbitMQ")
        except Exception as e:
            raise MessageMiddlewareMessageError(f"Error consuming messages: {e}")

    def stop_consuming(self):
        if hasattr(self, '_consuming') and self._consuming and self.channel:
            self.channel.stop_consuming()
            self._consuming = False

    def send(self, message):
        try:
            for routing_key in self.routing_keys:
                self.channel.basic_publish(
                    exchange=self.exchange_name,
                    routing_key=routing_key,
                    body=message,
                    properties=pika.BasicProperties(delivery_mode=2)
                )
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError("Lost connection to RabbitMQ")
        except Exception as e:
            raise MessageMiddlewareMessageError(f"Error sending message: {e}")

    def close(self):
        self.stop_consuming()
        if self.connection and not self.connection.is_closed:
            try:
                self.connection.close()
            except Exception as e:
                raise MessageMiddlewareCloseError(f"Error closing connection: {e}")
