import couchdb
import logging
import time
import json
import os
import numpy
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, FloatType, IntegerType
import pyspark.sql.functions as f
from time import perf_counter



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
        self.averages = [];
        self.workers = [[10,2],[50,5],[100,10]]

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
        for _id in self.db:
            self.debug(
                f"document id is {_id}")
            chunk = self.db.get(_id).get('results')

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
                count+=1
                # if count >= 10000:
                #     break
            self.debug(
                f"{len(chunks)} records created")
        return chunks

    def compute_average(self, chunks=[], property='work'):
        energySchema = StructType([
            StructField("id", IntegerType(), True),
            StructField("timestamp", IntegerType(), True),
            StructField("value", FloatType(), True),
            StructField("property", IntegerType(), True),
            StructField("plug_id", IntegerType(), True),
            StructField("household_id", IntegerType(), True),
            StructField("house_id", IntegerType(), True)])

        for worker in self.workers:
            i = 0
            while i < 10:
                mapped = SparkSession\
                    .builder\
                    .appName("AvgWorkMapReduce")\
                    .config('spark.default.parallelism',worker[0])\
                    .config('spark.sql.shuffle.partitions',worker[1])\
                    .getOrCreate()

                self.debug(
                    f"trying to create DataFrame")
                df = mapped.createDataFrame(chunks, energySchema)
                start_time = perf_counter()
                reduce = df.groupby(['house_id', 'household_id', 'plug_id']).agg(f.avg(f.when(df.property == 0, df.value)).alias('work'), f.avg(f.when(df.property == 1, df.value)).alias('load')).collect()
                end_time = perf_counter() 
                time_elapsed = end_time - start_time
                self.averages.append([worker[0],worker[1],time_elapsed,i])
                i += 1
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

        # Average Load and Work computation
        sumWork=0.0
        sumLoad=0.0
        n=0.0
        for res in results:
            sumWork+=float(res['work'])
            sumLoad+=float(res['load'])
            n+=1
        avgWork = sumWork / n;
        avgLoad = sumLoad / n;
        self.debug(f"average work: {avgWork}, average load: {avgLoad}")

        res = json.dumps(results)
        data = {}
        for i, res in enumerate(results):
            data[i] = res

        db.save(data);

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
    # avg_load_results = master.compute_average(
    #     chunks=master.get_chunks(),
    #     property='load'
    # )
    reduced_results = master.compute_average(
        chunks=master.get_chunks(),
        property='load'
    )
    results = json.dumps(reduced_results)
    # master.debug(f'{results}')
    for row in master.averages:
        master.debug(f'M: {row[0]} R: {row[1]} Time: {row[2]} #: {row[3]}')
    # Save results to couchdb
    master.save_to_db('reduced-results', reduced_results)
    # master.save_to_db('average-load', avg_load_results)