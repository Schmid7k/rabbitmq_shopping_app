import json
from db_and_event_definitions import ProductEvent

import pika
from pika import connection

from xprint import xprint


class ShoppingEventProducer:
    def __init__(self):
        self.connection = None
        self.channel = None

    def initialize_rabbitmq(self):
        # Initialize the RabbitMq connection, channel, exchange and queue here
        xprint("ShoppingEventProducer initialize_rabbitmq() called")
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host="localhost")
        )
        xprint("ShoppingEventProducer initialize_rabbitmq() connection created")
        self.channel = self.connection.channel()
        xprint("ShoppingEventProducer initialize_rabbitmq() connected to channel")
        self.channel.exchange_declare(
            exchange="shopping_events_exchange", exchange_type="x-consistent-hash"
        )

    def publish(self, shopping_event):
        xprint(
            "ShoppingEventProducer: Publishing shopping event {}".format(
                vars(shopping_event)
            )
        )
        message = json.dumps(vars(shopping_event))
        self.channel.basic_publish(
            exchange="shopping_events_exchange",
            routing_key=shopping_event.event_type,
            body=message,
        )

    def close(self):
        self.channel.close()
        self.connection.close()
