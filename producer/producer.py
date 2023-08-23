from kafka import KafkaProducer
import json
import time
import random
import uuid
from apscheduler.schedulers.background import BackgroundScheduler
from random import randrange
import datetime

def random_date(start, end):
    """
    This function will return a random datetime between two datetime
    objects.
    """
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = randrange(int_delta)
    return start + datetime.timedelta(seconds=random_second)

def get_user():
    return {
        "id": str(uuid.uuid4()),
        "sender_id": random.randint(1000, 9999999),
        "text": "Hi From Dataak!",
        "created_at": random_date(datetime.datetime.now() - datetime.timedelta(days=10), datetime.datetime.now()).isoformat(),
        "like_count": random.randint(0, 30),
        "user": {
            "id": random.randint(1000, 9999999),
            "username": "alex",
            "follower_count": random.randint(0, 2000)
        }
    }

producer = KafkaProducer(
    bootstrap_servers=['kafka:9093'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

def produce_data():
    data = [get_user() for _ in range(100)]
    for item in data:
        print(item)
        producer.send('users', value=item)

scheduler = BackgroundScheduler()
scheduler.add_job(produce_data, 'interval', minutes=1)
scheduler.start()

if __name__ == "__main__":
    while True:
        time.sleep(10)
