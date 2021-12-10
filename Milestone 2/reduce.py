import couchdb
import logging
import time
import json
import os
import numpy
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType,StructField, StringType, FloatType, IntegerType
import pyspark.sql.functions as f
import requests



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

    def database_exists(self, database):
        return database in self.couchserver
    
    # create couchdb connection
    def couch_connect(self):
        dbname = "energy-data"
        user = "admin"
        password = "16"
        self.couchserver = couchdb.Server("http://%s:%s@129.114.24.223:5984/" % (user, password))
        if dbname in self.couchserver:
            self.db = self.couchserver[dbname]
            self.debug(
                f"Successfully connected to existing CouchDB database energy-data")
        else:
            self.db = self.couchserver.create(dbname)
            self.debug(
                f"Successfully created new CouchDB database energy-data")

    def get_chunks(self):
        chunks = []
        for doc_id in self.db:
            self.debug(
                f"doc_id is {doc_id}")
            chunk = self.db.get(doc_id).get('results')

            count = 0;
            for record in chunk:
                tmp = []
                tmp.append(int(record[0]))
                tmp.append(int(record[1]))
                tmp.append(float(record[2]))
                tmp.append(int(record[3]))
                tmp.append(int(record[4]))
                tmp.append(int(record[5]))
                tmp.append(int(record[6]))
                chunks.append(tmp)
                if count > 100:
                    self.debug(
                        f"{len(chunks)} records created")
                    break
        return chunks
    def compute_average(self, chunks=[], property='work'):
        filterBy = {
            'work': '0',
            'load': '1'
        }
        energySchema = StructType([
            StructField("id", IntegerType(), True),
            StructField("timestamp", IntegerType(), True),
            StructField("value", FloatType(), True),
            StructField("property", IntegerType(), True),
            StructField("plug_id", IntegerType(), True),
            StructField("household_id", IntegerType(), True),
            StructField("house_id", IntegerType(), True)])

        # columns = ["id", "timestamp", "value", "property", "plug_id", "household_id", "house_id"]

        mapped = SparkSession\
            .builder\
            .appName("AvgWorkMapReduce")\
            .getOrCreate()

        self.debug(
            f"trying to create DataFrame")
        df = mapped.createDataFrame(chunks, energySchema)
        # reduced = df.groupby(['house_id', 'household_id', 'plug_id']).agg(f.avg(f.when(df.property == 0, df.value)).alias('work'), f.avg(f.when(df.property == 1, df.value)).alias('load')).collect()
        # reduce = mapped.aggregateByKey(
        #     zeroValue=(0, 0),

        #     sum=lambda a, b: (a[0] + b, a[1] + 1),
        #     count=lambda a, b: (a[0] + b[0], a[1] + b[1]))\
        #     .mapValues(lambda x: x[0]/x[1]).collect()  # basic avg computation
        reduce = df.groupby(['house_id', 'household_id', 'plug_id']).agg(f.avg(f.when(df.property == 0, df.value)).alias('work'), f.avg(f.when(df.property == 1, df.value)).alias('load')).collect()
        mapped.stop();
        reduce = [r.asDict() for r in reduce]
        return reduce

    def save_to_db(self, dbname, results):
        try:
            db = self.couchserver.create(dbname)
            self.debug(
                f"Successfully created new CouchDB database {dbname}")
        except:
            db = self.couchserver[dbname]
            self.debug(
                f"Successfully connected to existing CouchDB database {dbname}")
        self.debug(f'Preparing to save results to database')

        res = json.dumps(results)
        db.save(res);
        # chunked_list = numpy.array_split(results,2)

        # for result in results:
        #     try:
        #         payload = {
        #             'plug_id': result[0][0],
        #             'household_id': result[0][1],
        #             'house_id': result[0][2],
        #             'value': result[1]
        #         }
        #         db.save(payload)
        #         self.debug("Saved row")
        #     except Exception as e:
        #         self.error(e)
        # url = "http://admin:16@129.114.24.223:5984/"
        # for chunk in chunked_list:
        #     #convert to json
        #     docs = json.dumps({'docs': chunk.tolist()})
        #     print('Sending chunk.')
        #     # res = requests.post(url, headers=headers, data=docs)
        #     # print('Response body:', res.status_code)
        #     db.save(docs)

        # db.save(results)
        self.debug("Saving completed")

    def setup_logging(self, verbose):
        self.logger = logging.getLogger('EnergyMapReduce')
        formatter = logging.Formatter('%(prefix)s - %(message)s')
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        self.prefix = {'prefix': 'EnergyMapReduce'}
        self.logger.addHandler(handler)
        self.logger = logging.LoggerAdapter(self.logger, self.prefix)
        if verbose:
            self.logger.setLevel(logging.DEBUG)
            self.logger.debug('Debug mode enabled', extra=self.prefix)
        else:
            self.logger.setLevel(logging.INFO)

    def debug(self, msg):
        self.logger.debug(msg, extra=self.prefix)

    def info(self, msg):
        self.logger.info(msg, extra=self.prefix)

    def error(self, msg):
        self.logger.error(msg, extra=self.prefix)

if __name__ == "__main__":
    master = EnergyMapReduce(verbose=True)

    # while not master.database_exists("complete"):
    #     master.debug("waiting on 'complete' database creation")
    #     time.sleep(3)

    # Use mapreduce to get the average values for both properties 'work' and 'load'
    # avg_work_results = master.compute_average(
    #     chunks=master.get_chunks(),
    #     property='work'
    # )
    avg_load_results = master.compute_average(
        chunks=master.get_chunks(),
        property='load'
    )

    for result in avg_load_results:
        master.debug(f'M: {result[0]} R: {result[1]} I: {result[2]} Time: {result[3]}')
    # Save results to couchdb
    # master.save_to_db('average-work', avg_work_results)
    master.save_to_db('average-load', avg_load_results)