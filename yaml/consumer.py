#
#
# Author: Aniruddha Gokhale
# CS4287-5287: Principles of Cloud Computing, Vanderbilt University
#
# Created: Sept 6, 2020
#
# Purpose:
#
#    Demonstrate the use of Kafka Python streaming APIs.
#    In this example, demonstrate Kafka streaming API to build a consumer.
#

import os   # need this for popen
import time # for sleep
from kafka import KafkaConsumer  # consumer of events
from json import loads
import couchdb
import json

# We can make this more sophisticated/elegant but for now it is just
# hardcoded to the setup I have on my local VMs

# acquire the consumer
# (you will need to change this to your bootstrap server's IP addr)
consumer = KafkaConsumer (
		bootstrap_servers="localhost:9092",
		value_deserializer=lambda x: loads(x.decode('utf-8')))

# subscribe to topic
consumer.subscribe (topics=["utilizations"])

user = "admin"
password = "team16"
couchserver = couchdb.Server("http://%s:%s@129.114.24.223:5984/" % (user, password))
dbname = "topicdata"
if dbname in couchserver:
    db = couchserver[dbname]
else:
    db = couchserver.create(dbname)

data = {}

# we keep reading and printing
for i, msg in enumerate(consumer):
    print (msg.value)
    data[i] = msg.value
    if (i > 10):
       break
#    db.save(value)
# we are done. As such, we are not going to get here as the above loop
# is a forever loop.
db.save(data)
print ("done")
consumer.close ()






