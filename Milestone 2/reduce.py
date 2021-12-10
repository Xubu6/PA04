import couchdb
import argparse
import logging
import time
import json
import os
from pyspark.sql import SparkSession, Row

# spark = SparkSession\
#     .builder\
#     .appName("PythonWordCount")\
#     .getOrCreate()

# lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
# counts = lines.flatMap(lambda x: x.split(' ')) \
#                 .map(lambda x: (x, 1)) \
#                 .reduceByKey(add)
# output = counts.collect()
# for (word, count) in output:
#     print("%s: %i" % (word, count))


class EnergyMapReduce:
    def __init__(self, verbose=False):
        self.setup_logging(verbose=verbose)
        self.couch_connect()
    
    # create couchdb connection
    def couch_connect(self):
        dbname = "energy-data"
        user = "admin"
        password = "16"
        self.couchserver = couchdb.Server("http://%s:%s@129.114.24.223:5984/" % (user, password))
        if dbname in self.couchserver:
            self.db = self.couchserver[dbname]
        else:
            self.db = self.couchserver.create(dbname)

    def get_chunks(self):
        chunks = []
        for _id in self.db:
            chunk = self.db.get(_id).get('chunk')
            # This chunk contains many sub dicts, each of which represent an energy record from CSV
            for energy_record in chunk:
                chunks.append(energy_record)
        return chunks

    def compute_average(self, chunks=[], property='work'):
        filterBy = {
            'work': '0',
            'load': '1'
        }
        mapped = SparkSession\
            .builder\
            .appName("AvgWorkMapReduce")\
            .getOrCreate()\
            .createDataFrame(
                Row(
                    id=int(x[0]),
                    timestamp=x[1],
                    value=float(x[2]),
                    property=int(x[3]),
                    plug_id=int(x[4]),
                    household_id=int(x[5]),
                    house_id=int(x[6])
                ) for x in chunks if x[3] == filterBy[property]).rdd.map(
                lambda row: (
                    (row.plug_id, row.household_id, row.house_id), row.value
                )
            )
        reduce = mapped.aggregateByKey(
            zeroValue=(0, 0),

            sum=lambda a, b: (a[0] + b,    a[1] + 1),
            count=lambda a, b: (a[0] + b[0], a[1] + b[1]))\
            .mapValues(lambda x: x[0]/x[1]).collect()  # basic avg computation
        return reduce

    def save_to_db(self, dbname, results):
        try:
            db = self.couchserver.create(dbname)
        except:
            db = self.couchserver[dbname]
            self.debug("Connected to db")
        self.debug('Attempting to save to db')

        for result in results:
            try:
                payload = {
                    'plug_id': result[0][0],
                    'household_id': result[0][1],
                    'house_id': result[0][2],
                    'value': result[1]
                }
                db.save(payload)
            except Exception as e:
                self.error(e)
        self.debug("Saving completed")

if __name__ == "__main__":
    master = EnergyMapReduce(verbose=True)

    master.info("CouchDB Server is now ready, beginning Spark Analysis")
    # Use mapreduce to get the average values for both properties 'work' and 'load'
    avg_work_results = master.compute_average(
        chunks=master.get_chunks(),
        property='work'
    )
    avg_load_results = master.compute_average(
        chunks=master.get_chunks(),
        property='load'
    )
    # Save results to couchdb
    master.save_to_db('average-work', avg_work_results)
    master.save_to_db('average-load', avg_load_results)