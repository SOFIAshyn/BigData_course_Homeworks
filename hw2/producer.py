from kafka import KafkaProducer
from json import dumps
import csv
import datetime

producer = KafkaProducer(bootstrap_servers='10.128.0.6:9092')

with open('./training.1600000.processed.noemoticon.csv', 'r') as file:
    csv_reader = csv.reader(file)
    for messages in csv_reader:
        # print(messages)
        messages.insert(0, str(datetime.datetime.now()))
        producer.send('Twitter_Messages_Topic', str(messages).encode('utf-8'))
