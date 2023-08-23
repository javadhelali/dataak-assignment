from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
import json

es = Elasticsearch(['elasticsearch:9200'])
consumer = KafkaConsumer(
    'users',
    bootstrap_servers=['kafka:9093'],
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

for message in consumer:
    body = message.value
    print(message)
    print(body)
    es.index(index="users-posts", id=body["id"], doc_type='_doc', body=body)
