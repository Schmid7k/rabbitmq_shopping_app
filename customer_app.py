from types import SimpleNamespace
import pika
import json
from db_and_event_definitions import ProductEvent, BillingEvent
import time
import logging

from xprint import xprint


class CustomerEventConsumer:
    def __init__(self, customer_id):
        self.customer_id = customer_id
        self.connection = None
        self.channel = None
        self.temporary_queue_name = None
        self.shopping_events = []
        self.billing_events = []

    def initialize_rabbitmq(self):
        xprint(
            "CustomerEventConsumer {}: initialize_rabbitmq() called".format(
                self.customer_id
            )
        )
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host="localhost")
        )
        xprint(
            "CustomerEventConsumer {}: initialize_rabbitmq() connection created".format(
                self.customer_id
            )
        )
        self.channel = self.connection.channel()
        xprint(
            "CustomerEventConsumer {}: initialize_rabbitmq() connected to channel".format(
                self.customer_id
            )
        )
        self.channel.exchange_declare(
            exchange="customer_app_events", exchange_type="topic"
        )
        xprint(
            "CustomerEventConsumer {}: initialize_rabbitmq() declared channel".format(
                self.customer_id
            )
        )
        result = self.channel.queue_declare(queue="", exclusive=True)
        self.temporary_queue_name = result.method.queue
        xprint(
            "CustomerEventConsumer {}: initialize_rabbitmq() declared temporary queue".format(
                self.customer_id
            )
        )
        self.channel.queue_bind(
            exchange="customer_app_events",
            queue=self.temporary_queue_name,
            routing_key=self.customer_id,
        )

    def handle_event(self, ch, method, properties, body):
        xprint(
            "CustomerEventConsumer {}: handle_event() called".format(self.customer_id)
        )
        # Get message body
        received = json.loads(body)
        xprint(
            "CustomerEventConsumer {}: Received event {}".format(
                self.customer_id, received
            )
        )
        if "event_type" in received.keys():
            event = ProductEvent(
                event_type=received["event_type"],
                product_number=received["product_number"],
                timestamp=received["timestamp"],
            )
            self.shopping_events.append(event)
            xprint(
                "CustomerEventConsumer {}: Handling ProductEvent".format(
                    self.customer_id
                )
            )
        else:
            event = BillingEvent(
                customer_id=received["customer_id"],
                product_number=received["product_number"],
                pickup_time=received["pickup_time"],
                purchase_time=received["purchase_time"],
                shopping_cost=received["shopping_cost"],
            )
            self.billing_events.append(event)
            xprint(
                "CustomerEventConsumer {}: Handling BillingEvent".format(
                    self.customer_id
                )
            )

    def start_consuming(self):
        # Start consuming from Rabbit
        xprint(
            "CustomerEventConsumer {}: start_consuming() called".format(
                self.customer_id
            )
        )
        self.channel.basic_consume(
            queue=self.temporary_queue_name,
            on_message_callback=self.handle_event,
            auto_ack=True,
        )
        self.channel.start_consuming()

    def close(self):
        try:
            if self.channel is not None:
                print("CustomerEventConsumer {}: Closing".format(self.customer_id))
                self.channel.stop_consuming()
                time.sleep(1)
                self.channel.close()
            if self.connection is not None:
                self.connection.close()
        except Exception as e:
            print(
                "CustomerEventConsumer {}: Exception {} on close()".format(
                    self.customer_id, e
                )
            )
            pass
