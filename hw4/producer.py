###
#    Send requests to streaming platform
#    Send answers to Spark streaming to process
###
from kafka import KafkaProducer
import requests
import sys


if __name__ == "__main__":
	print('Producer is started ...')
	producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
	# print(f'Producer is started on host: localhost:9092')

	r = requests.get("https://stream.meetup.com/2/rsvps", stream=True)

	for line in r.iter_lines():
		# print(f'We have a line\n{line}')
		try:
			producer.send('meetsUp', line)
		except Exception as er:
			print(er)