import logging
import os

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class AggregationFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.fruit_top_by_client = {}
        self.eof_count_by_client = {}
        self.client_id = None

    def _process_data(self, client_id, fruit, amount):
        logging.info("Processing data message")
        fruit_top = self.fruit_top_by_client.setdefault(client_id, [])
        for index, current_fruit in enumerate(fruit_top):
            if current_fruit.fruit == fruit:
                fruit_top[index] = current_fruit + fruit_item.FruitItem(fruit, amount)
                fruit_top.sort(reverse=True)
                return
        fruit_top.append(fruit_item.FruitItem(fruit, amount))
        fruit_top.sort(reverse=True)

    def _process_eof(self, client_id):
        logging.info("Received EOF")
        self.client_id = client_id
        eof_count = self.eof_count_by_client.get(client_id, 0) + 1
        self.eof_count_by_client[client_id] = eof_count

        if eof_count < SUM_AMOUNT:
            return

        fruit_top = self.fruit_top_by_client.get(client_id, [])
        fruit_chunk = sorted(fruit_top, reverse=True)[:TOP_SIZE]
        fruit_top_payload = list(
            map(
                lambda fruit_item_entry: (fruit_item_entry.fruit, fruit_item_entry.amount),
                fruit_chunk,
            )
        )
        self.output_queue.send(message_protocol.internal.serialize([client_id] + fruit_top_payload))
        self.fruit_top_by_client.pop(client_id, None)
        self.eof_count_by_client.pop(client_id, None)

    def process_messsage(self, message, ack, nack):
        logging.info("Process message")
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            self._process_data(*fields)
        else:
            self._process_eof(*fields)
        ack()

    def start(self):
        self.input_exchange.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()
