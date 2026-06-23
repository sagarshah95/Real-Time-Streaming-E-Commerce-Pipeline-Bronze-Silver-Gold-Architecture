from confluent_kafka import Producer
from faker import Faker
import json
import time
import os
from dotenv import load_dotenv

load_dotenv()


BOOTSTRAP_SERVER = os.getenv("BOOTSTRAP_SERVER")
TOPIC = os.getenv("TOPIC")

fake = Faker()
producer_config = {'bootstrap.servers':BOOTSTRAP_SERVER}
producer = Producer(producer_config)

def delivery_report(err, msg):
    if err:
        print(f"Error while streaming :{err}")
    else:
        print(f"Delivered:{msg.value().decode('utf-8')}")


def generate_events():
    return {
        "user_id":fake.uuid4(),
        "event_type":fake.random_element(elements=["login", "click_nav", "purchase", "logout"]),
        "product_id":fake.pyint(min_value=1000, max_value=9999),
        "amount":fake.random_number(digits=4),
        "event_timestamp":fake.iso8601()
    }

def stream_events():
    
    print("Stream is Starting")
    try:
        while True:
            event = generate_events()
            producer.produce(
                topic=TOPIC,
                value=json.dumps(event).encode("utf-8"),
                callback=delivery_report
            )
            producer.poll(0)
            time.sleep(1)

        
    except KeyboardInterrupt:
        producer.flush()
        print("\nStream Stopped")


if __name__ == "__main__":
    stream_events()