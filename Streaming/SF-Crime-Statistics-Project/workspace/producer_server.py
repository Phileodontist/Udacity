from kafka import KafkaProducer
import json
import time


class ProducerServer(KafkaProducer):

    def __init__(self, input_file, topic, **kwargs):
        super().__init__(**kwargs)
        self.input_file = input_file
        self.topic = topic
        
    # Read each object and encodes it as binary
    def generate_data(self):
        with open(self.input_file) as f:
            records = json.load(f)
            for record in records:
                message = self.dict_to_binary(record)
                self.send(self.topic, message)
                time.sleep(1)

    # Encodes dictionaries to binary
    def dict_to_binary(self, json_dict):
        binaryDict = json.dumps(json_dict).encode('utf-8')
        return binaryDict
