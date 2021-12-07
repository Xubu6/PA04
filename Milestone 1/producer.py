#
#
# Author: Team 16
# CS4287-5287: Principles of Cloud Computing, Vanderbilt University
#
# Created: Nov 12, 2020
#
# Purpose:
#
#    Demonstrate the use of Kafka Python streaming APIs.
#    In this example, we use the "top" command and use it as producer of events for
#    Kafka. The consumer can be another Python program that reads and dumps the
#    information into a database OR just keeps displaying the incoming events on the
#    command line consumer (or consumers)
#

import os   # need this for popen
import time # for sleep
from kafka import KafkaProducer  # producer of events
from csv import reader
from json import dumps

# We can make this more sophisticated/elegant but for now it is just
# hardcoded to the setup I have on my local VMs

# acquire the producer
producer = KafkaProducer (bootstrap_servers="129.114.25.102:9092",
                                          acks=1,
                                          value_serializer= lambda x:
                                          dumps(x).encode('utf-8'))  # wait for leader to write to log

# say we send the contents 100 times after a sleep of 1 sec in between
with open('/Data/energy-sorted1M-1.csv', 'r') as read_obj:
	csv_reader = reader(read_obj)
	for i, row in enumerate(csv_reader):
		if i > 1000:
			break
		content = row
		producer.send('utilizations', value=content)
		print('Sent Row')
		producer.flush()
		# sleep a second
		

	# we are done
	producer.close ()




