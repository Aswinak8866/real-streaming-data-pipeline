import json
import random
import time
from datetime import datetime

from kafka import KafkaProducer


EVENT_TYPES = [
    "view_product",
    "add_to_cart",
    "purchase",
    "payment"
]


producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)


def generate_event():

    event = {
        "event_id": f"evt_{random.randint(100000, 999999)}",
        "user_id": random.randint(1, 1000),
        "event_type": random.choice(EVENT_TYPES),
        "product_id": f"P{random.randint(100, 999)}",
        "amount": round(random.uniform(100, 5000), 2),
        "timestamp": datetime.now().isoformat()
    }

    return event


while True:

    event = generate_event()

    producer.send(
        "ecommerce-events",
        value=event
    )

    print("Sent:", event)

    time.sleep(1)
