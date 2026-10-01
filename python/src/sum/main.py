import os
import logging
import threading

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

class SumFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)
        self.control_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, f"{SUM_CONTROL_EXCHANGE}_{ID}"
        )
        self.control_queues = [
            middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, f"{SUM_CONTROL_EXCHANGE}_{i}"
            )
            for i in range(SUM_AMOUNT)
        ]
        self.amount_by_fruit_by_client = {}
        self.finalized_clients = set()

    def _process_data(self, client_id, fruit, amount):
        logging.info("Process data")
        client_amounts = self.amount_by_fruit_by_client.setdefault(client_id, {})
        client_amounts[fruit] = client_amounts.get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))

    def _process_eof(self, client_id):
        if client_id in self.finalized_clients:
            return
        if client_id not in self.amount_by_fruit_by_client:
            return

        logging.info("Broadcasting data messages")
        for final_fruit_item in self.amount_by_fruit_by_client[client_id].values():
            for data_output_exchange in self.data_output_exchanges:
                data_output_exchange.send(
                    message_protocol.internal.serialize(
                        [client_id, final_fruit_item.fruit, final_fruit_item.amount]
                    )
                )

        logging.info("Broadcasting EOF message")
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(message_protocol.internal.serialize([client_id]))

        self.amount_by_fruit_by_client.pop(client_id, None)
        self.finalized_clients.add(client_id)

    def _broadcast_eof(self, client_id):
        if SUM_AMOUNT <= 1:
            return
        eof_message = message_protocol.internal.serialize([client_id])
        for control_queue in self.control_queues:
            control_queue.send(eof_message)

    def process_data_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            self._process_data(*fields)
        else:
            client_id = fields[0]
            self._process_eof(client_id)
            if SUM_AMOUNT > 1:
                self._broadcast_eof(client_id)
        ack()

    def process_control_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 1:
            self._process_eof(*fields)
        ack()

    def start(self):
        if SUM_AMOUNT == 1:
            self.input_queue.start_consuming(self.process_data_messsage)
            return

        data_thread = threading.Thread(
            target=self.input_queue.start_consuming,
            args=(self.process_data_messsage,),
            daemon=True,
        )
        data_thread.start()
        self.control_queue.start_consuming(self.process_control_message)

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
