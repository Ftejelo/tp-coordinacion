import os
import logging
import socket
import signal
import multiprocessing
import itertools
import message_handler
from common import middleware, message_protocol

SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]


def handle_client_request(client_socket, message_handler):
    output_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, OUTPUT_QUEUE)

    try:
        while True:
            message = message_protocol.external.recv_msg(client_socket)

            if message[0] == message_protocol.external.MsgType.FRUIT_RECORD:
                serialized_message = message_handler.serialize_data_message(message[1])
                output_queue.send(serialized_message)
                message_protocol.external.send_msg(
                    client_socket, message_protocol.external.MsgType.ACK
                )

            if message[0] == message_protocol.external.MsgType.END_OF_RECODS:
                serialized_message = message_handler.serialize_eof_message(message[1])
                output_queue.send(serialized_message)
                message_protocol.external.send_msg(
                    client_socket, message_protocol.external.MsgType.ACK
                )
                return
    except socket.error:
        logging.error("The connection with the server was lost")
    except Exception as e:
        logging.error(e)
    finally:
        output_queue.close()


def handle_client_response(client_list):
    input_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, INPUT_QUEUE)

    def _consume_result(message, ack, nack):
        try:
            fields = message_protocol.internal.deserialize(message)
            client_id = fields[0] if fields else None
            fruit_top = fields[1:] if fields else []
            
            client_index = -1
            for i, [message_handler_instance, client_socket] in enumerate(client_list):
                if message_handler_instance.client_id == client_id:
                    client_index = i
                    break
            
            if client_index == -1:
                logging.error(f"Client ID {client_id} not found in client list")
                ack()
                return
            
            message_handler_instance, client_socket = client_list[client_index]
            
            message_protocol.external.send_msg(
                client_socket,
                message_protocol.external.MsgType.FRUIT_TOP,
                fruit_top,
            )
            message_protocol.external.recv_msg(client_socket)
            client_list.pop(client_index)
            ack()
        except socket.error:
            logging.error("The connection with the server was lost")
            if client_index >= 0:
                client_list.pop(client_index)
            ack()
        except Exception as e:
            logging.error(e)
            nack()
            input_queue.stop_consuming()

    input_queue.start_consuming(_consume_result)
    input_queue.close()


def handle_sigterm(server_socket, client_list, sigterm_received):
    server_socket.shutdown(socket.SHUT_RDWR)
    for [_, client_socket] in client_list:
        client_socket.shutdown(socket.SHUT_RDWR)
    sigterm_received.value = 1


def main():
    logging.basicConfig(level=logging.INFO)

    with multiprocessing.Manager() as manager:
        client_list = manager.list()
        sigterm_received = manager.Value("c_short", 0)
        client_id_counter = itertools.count(1)
        with multiprocessing.Pool(processes=os.process_cpu_count()) as processes_pool:
            processes_pool.apply_async(handle_client_response, (client_list,))

            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
                logging.info("Listening to connections")
                server_socket.bind((SERVER_HOST, SERVER_PORT))
                server_socket.listen()
                signal.signal(
                    signal.SIGTERM,
                    lambda signum, frame: handle_sigterm(
                        server_socket, client_list, sigterm_received
                    ),
                )
                while True:
                    try:
                        client_socket, _ = server_socket.accept()

                        logging.info("A new client has connected")
                        message_handler_instance = message_handler.MessageHandler()
                        message_handler_instance.client_id = next(client_id_counter)
                        client_list.append([message_handler_instance, client_socket])
                        processes_pool.apply_async(
                            handle_client_request,
                            (client_socket, message_handler_instance),
                        )
                    except socket.error:
                        if sigterm_received.value == 0:
                            logging.error("The connection with the client was lost")
                            return 1
                        else:
                            return 0
                    except Exception as e:
                        logging.error(e)
                        return 2
    return 0


if __name__ == "__main__":
    main()
