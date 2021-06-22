import pyspark
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import sys
from kafka import KafkaProducer
from pyspark import SparkContext
import json
from datetime import datetime
from constants import topic_name1, topic_name2, topic_name3
from constants import sub_topics, states_constants


MAX_NUM = 999999999999999


def producer_send(message, topic, producer):
    records = message.take(MAX_NUM)
    for record in records:
        producer.send(topic, str(record))


def date_format(el):
    cur_t = datetime.now()
    return {
        "cities": [el["group"]["group_city"]],
        "day": cur_t.day,
        "month": cur_t.month,
        "minute": cur_t.minute,
        "hour": cur_t.hour,
        "year": cur_t.year
    }


def is_in_sub_topics(x):
    for topic in x["group"]["group_topics"]:
        if topic["topic_name"] in sub_topics:
            return True
    return False


def aggregation(data1, data2):
    data1["cities"] += data2["cities"]
    return data1


if __name__ == "__main__":
    print(pyspark.__version__)
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                             value_serializer=lambda x: x.encode('UTF-8'))
    # print(f'We started a producer on host {str(sys.argv[1])}')

    sparkContext = SparkContext("local[*]", "Assignment4")
    sparkContext.setLogLevel("ERROR")

    streamingSparkContext = StreamingContext(sparkContext, 5)
    kafkaUtils = KafkaUtils.createStream(streamingSparkContext, 'localhost:2181', "1", {"meetsUp": 3})

    usMeetUpsStreaming = kafkaUtils.map(lambda el: json.loads(el[1])) \
        .filter(lambda el: el['group']['group_country'] == 'us')

    preparedUsMeetUps = usMeetUpsStreaming.map(lambda el: {
        "event": {
            "event_name": el["event"]["event_name"],
            "event_id": el["event"]["event_id"],
            "time": datetime.strftime(datetime.fromtimestamp((el["event"]["time"]) / 1000.0), "%Y.%m.%d-%H:%M:%S")

        },
        "group_city": el["group"]["group_city"],
        "group_id": el["group"]["group_id"],
        "group_name": el["group"]["group_name"],
        "group_state": states_constants[el["group"]["group_state"]],
    }) \
        .foreachRDD(lambda el: producer_send(el, topic_name1, producer))

    usCities = usMeetUpsStreaming.window(60, 60) \
        .map(date_format) \
        .reduce(aggregation).foreachRDD(lambda el: producer_send(el, topic_name2, producer))

    compProgrMeetUps = usMeetUpsStreaming.filter(is_in_sub_topics).map(lambda el: {
        "event": {
            "event_name": el["event"]["event_name"],
            "event_id": el["event"]["event_id"],
            "time": datetime.strftime(datetime.fromtimestamp((el["event"]["time"]) / 1000.0), "%Y.%m.%d-%H:%M:%S")

        },
        "group_topics": list(map(lambda new_el: new_el["topic_name"], el["group"]["group_topics"])),
        "group_city": el["group"]["group_city"],
        "group_id": el["group"]["group_id"],
        "group_name": el["group"]["group_name"],
        "group_state": states_constants[el["group"]["group_state"]],
    }) \
        .foreachRDD(lambda el: producer_send(el, topic_name3, producer))
    # print('We are here')
    streamingSparkContext.start()
    print('StreamingContext was started ...')
    streamingSparkContext.awaitTermination()