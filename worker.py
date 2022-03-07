from types import SimpleNamespace

import pika
import json
import dateutil.parser
import time
from db_and_event_definitions import (
    customers_database,
    cost_per_unit,
    number_of_units,
    BillingEvent,
    ProductEvent,
)
from xprint import xprint


class ShoppingWorker:
    def __init__(self, worker_id, queue, weight="1"):
        self.connection = None
        self.channel = None
        self.worker_id = worker_id
        self.queue = queue
        self.weight = weight
        self.shopping_state = {}
        self.shopping_events = []
        self.billing_event_producer = None
        self.customer_app_event_producer = None

    def initialize_rabbitmq(self):
        # Initialize the RabbitMQ connection, channel, exchange and queue here
        # Also initialize the channels for the billing_event_producer and customer_app_event_producer
        xprint("ShoppingWorker {}: initialize_rabbitmq() called".format(self.worker_id))
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host="localhost")
        )
        # Initialize channel and exchange
        self.channel = self.connection.channel()
        self.channel.exchange_declare(
            exchange="shopping_events_exchange", exchange_type="x-consistent-hash"
        )
        # Initialize deadletter queue
        result = self.channel.queue_declare("shopping_events_dead_letter_queue")

        # Initialize worker queue
        result = self.channel.queue_declare(
            queue=self.queue,
            arguments={
                "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key": "shopping_events_dead_letter_queue",
            },
        )
        queue_name = result.method.queue
        self.channel.queue_bind(
            exchange="shopping_events_exchange",
            queue=queue_name,
            routing_key=self.weight,
        )
        # Initialize CustomerEventProducer
        self.customer_app_event_producer = CustomerEventProducer(
            connection=self.connection, worker_id=self.worker_id
        )
        # Initialize BillingEventProducer
        self.billing_event_producer = BillingEventProducer(
            connection=self.connection, worker_id=self.worker_id
        )

        self.customer_app_event_producer.initialize_rabbitmq()
        self.billing_event_producer.initialize_rabbitmq()

    def handle_shopping_event(self, ch, method, properties, body):
        # This is the callback that is passed to "on_message_callback" when a message is received
        xprint("ShoppingWorker {}: handle_event() called".format(self.worker_id))
        # Handle the application logic and the publishing of events here
        # Get message body
        received = json.loads(body)
        # Turn message body into ProductEvent object
        event = ProductEvent(
            event_type=received["event_type"],
            product_number=received["product_number"],
            timestamp=received["timestamp"],
        )
        xprint(
            "ShoppingWorker {}: Received {}".format(
                self.worker_id, event.product_number
            )
        )

        # Retrieve customer_id from ProductEvent
        customer_id = self.get_customer_id_from_shopping_event(event)
        # Dead letter unknown product
        if customer_id == None:
            xprint("Unknown customer id!")
            ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
        else:
            xprint(
                "ShoppingWorker {}: Request from {}".format(self.worker_id, customer_id)
            )

            # Append ProductEvent to shopping_events list
            self.shopping_events.append(event)
            xprint(
                "ShoppingWorker {}: Added new event to list. New list length {}".format(
                    self.worker_id, len(self.shopping_events)
                )
            )

            # Produce CustomerAppEvent
            self.customer_app_event_producer.publish_shopping_event(
                customer_id=customer_id, shopping_event=event
            )

            # Save customer basket to dictionary
            if event.event_type == "pick up":
                self.shopping_state[event.product_number] = event.timestamp
                xprint(
                    "ShoppingWorker {}: Saving product {} picked up at {} to basket".format(
                        self.worker_id, event.product_number, event.timestamp
                    )
                )

            if event.event_type == "purchase":
                billing_event = BillingEvent(
                    customer_id=customer_id,
                    product_number=event.product_number,
                    pickup_time=self.shopping_state[event.product_number],
                    purchase_time=event.timestamp,
                    shopping_cost=(cost_per_unit * number_of_units) * 0.8,
                )
                self.billing_event_producer.publish(billing_event=billing_event)
                self.customer_app_event_producer.publish_billing_event(
                    billing_event=billing_event
                )
                # Remove item from dictionary after a purchase
                self.shopping_state.pop(event.product_number)
                xprint(
                    "ShoppingWorker {}: Removing product {} purchased at {} from basket".format(
                        self.worker_id, event.product_number, event.timestamp
                    )
                )
            ch.basic_ack(delivery_tag=method.delivery_tag)

    # Utility function to get the customer_id from a shopping event
    def get_customer_id_from_shopping_event(self, shopping_event):
        customer_id = [
            customer_id
            for customer_id, product_number in customers_database.items()
            if shopping_event.product_number == product_number
        ]
        if len(customer_id) is 0:
            xprint(
                "{}: Customer Id for product number {} Not found".format(
                    self.worker_id, shopping_event.product_number
                )
            )
            return None
        return customer_id[0]

    def start_consuming(self):
        # Start consuming from Rabbit
        xprint("ShoppingWorker {}: start_consuming() called".format(self.worker_id))
        self.channel.basic_consume(
            queue=self.queue,
            on_message_callback=self.handle_shopping_event,
        )
        self.channel.start_consuming()

    def close(self):
        try:
            xprint("Closing worker with id = {}".format(self.worker_id))
            self.channel.stop_consuming()
            time.sleep(1)
            self.channel.close()
            self.billing_event_producer.close()
            self.customer_app_event_producer.close()
            time.sleep(1)
            self.connection.close()
        except Exception as e:
            print(
                "Exception {} when closing worker with id = {}".format(
                    e, self.worker_id
                )
            )


class BillingEventProducer:
    def __init__(self, connection, worker_id):
        self.worker_id = worker_id
        # Reusing connection created in ShoppingWorker
        self.channel = connection.channel()

    def initialize_rabbitmq(self):
        # Initialize the RabbitMq connection, channel, exchange and queue here
        xprint(
            "BillingEventProducer {}: initialize_rabbitmq() called".format(
                self.worker_id
            )
        )
        self.channel.queue_declare(queue="billing_events")

    def publish(self, billing_event):
        xprint(
            "BillingEventProducer {}: Publishing billing event {}".format(
                self.worker_id, vars(billing_event)
            )
        )
        # Publish a message to the Rabbitmq here
        # Use json.dumps(vars(billing_event)) to convert the shopping_event object to JSON
        message = json.dumps(vars(billing_event))
        self.channel.basic_publish(
            exchange="", routing_key="billing_events", body=message
        )
        xprint(
            "BillingEventProducer {}: Sent billing event {}".format(
                self.worker_id, vars(billing_event)
            )
        )

    def close(self):
        self.channel.close()


class CustomerEventProducer:
    def __init__(self, connection, worker_id):
        self.worker_id = worker_id
        # Reusing connection created in ShoppingWorker
        self.channel = connection.channel()

    def initialize_rabbitmq(self):
        # Initialize the RabbitMq connection, channel, exchange and queue here
        xprint(
            "CustomerEventProducer {}: initialize_rabbitmq() called".format(
                self.worker_id
            )
        )
        self.channel.exchange_declare(
            exchange="customer_app_events", exchange_type="topic"
        )
        xprint("CustomerEventProducer initialize_rabbitmq() declared exchange")

    def publish_billing_event(self, billing_event):
        xprint(
            "{}: CustomerEventProducer: Publishing billing event {}".format(
                self.worker_id, vars(billing_event)
            )
        )
        # Get routing_key from shopping_event
        message = json.dumps(vars(billing_event))
        # Publish message to customer_app_events
        self.channel.basic_publish(
            exchange="customer_app_events",
            routing_key=billing_event.customer_id,
            body=message,
        )
        # Publish a message to the Rabbitmq here
        # Use json.dumps(vars(billing_event)) to convert the shopping_event object to JSON

    def publish_shopping_event(self, customer_id, shopping_event):
        xprint(
            "{}: CustomerEventProducer: Publishing shopping event {} {}".format(
                self.worker_id, customer_id, vars(shopping_event)
            )
        )
        message = json.dumps(vars(shopping_event))
        # Publish message to customer_app_events
        self.channel.basic_publish(
            exchange="customer_app_events",
            routing_key=customer_id,
            body=message,
        )
        # Publish a message to the Rabbitmq here
        # Use json.dumps(vars(shopping_event)) to convert the shopping_event object to JSON

    def close(self):
        self.channel.close()
