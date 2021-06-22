###
#    Gets processed results
#    For one task we have to run one consumer:
#    'python consumer.py 1'
#    'python consumer.py 2'
#    'python consumer.py 3'
###
from kafka import KafkaConsumer
import json
import sys
import time
import ast
import os

config1 = {
    "bucket_name": "hw4",
    "output": "1.json",
    "topic": "US-meetups",
    "delay": 600,
}

config2 = {
    "bucket_name": "hw4",
    "output": "2.json",
    "topic": "US-cities-every-minute",
    "delay": 600,
}

config3 = {
    "bucket_name": "hw4",
    "output": "3.json",
    "topic": "Programming-meetups",
    "delay": 600,
}


if __name__ == "__main__":
    if sys.argv[1] == '2':
        config = config2
    elif sys.argv[1] == '3':
        config = config3
    else:
        config = config1

    print('Consumer is started ...')

    consumer = KafkaConsumer(
        config['topic'],
        bootstrap_servers=['localhost:9092'],
        value_deserializer=lambda x: ast.literal_eval(x.decode('UTF-8'))
    )
    # print(
    #     f"Started consumer on topic: {config['topic']}, on host: localhost:9092, time duration of consumer work is {config['time_duration']}")
    # print(f"File out = {config['file_out']}")

    res = []
    start = time.time()
    for message in consumer:
        print(message.value)
        res.append(message.value)
        if (time.time() - start) >= config["delay"]:
            break

    print('We are creating an output file...')
    f = open(config['output'], 'w+')
    json.dump(res, f, indent=2)
    f.close()