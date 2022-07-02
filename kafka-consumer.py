from kafka import KafkaConsumer
import json

def func():

    consumer = KafkaConsumer(
        'topic_name',
        bootstrap_servers='localhost:9092',
        group_id='consumer-group-a',
        auto_offset_reset='earliest'
    )
    print("consumer got created")
    for msg in consumer:
        a = f"Message received is: {json.loads(msg.value)}"


if __name__=="__main__":
    func()