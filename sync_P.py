from kafka import KafkaProducer
from kafka.errors import KafkaError
import json

class MyProducer:
    def __init__(self,location,bootstrap_server_list):
        self.counter=0
        self.bootstrap_server_list=bootstrap_server_list
        self.location= location

    def send(self):
        KafkaProducer(bootstrap_servers=self.bootstrap_server_list)
        producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('utf-8'))
        with open(self.location) as complete_file:
            #chunking for large file sending problem
            for chunks in json.loads(complete_file.read()):
                producer.send('json-topic', value=chunks)
                self.counter=self.counter+1
	return self.counter

Producer=MyProducer("MOCK.json",['localhost:9092'])
print("number of sent messages is %s"%Producer.send())



