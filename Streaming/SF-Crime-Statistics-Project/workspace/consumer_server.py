from kafka import KafkaConsumer

consumer = KafkaConsumer('police-calls', bootstrap_servers=['localhost:9092'], auto_offset_reset='earliest')

for message in consumer:
    print(message.value)