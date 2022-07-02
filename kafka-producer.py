from kafka import KafkaProducer
import json
import time
from faker import Faker

fake = Faker()

def get_registered_user():
    return {"name": fake.name(), "address": fake.address(), "created_in": fake.year()}

def json_serializer(data):
    return json.dumps(data)

def get_partition():
    return 0 #always sending to partition 0

producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=json_serializer
                         , partitioner=get_partition)

if __name__ == "__main__":
    while 1==1:
        message = get_registered_user()
        producer.send("topic_name", message)
        time.sleep(4)
