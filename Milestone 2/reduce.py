import couchdb
import argparse
import logging
import time
import json
import os
from pyspark.sql import SparkSession, Row, SQLContext
from pyspark.sql.types import *
import pandas as pd


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
    def equivalent_type(self, f):
        if f == 'datetime64[ns]': return TimestampType()
        elif f == 'int64': return LongType()
        elif f == 'int32': return IntegerType()
        elif f == 'float64': return FloatType()
        else: return StringType()

    def define_structure(self, string, format_type):
        try: typo = self.equivalent_type(format_type)
        except: typo = StringType()
        return StructField(string, typo)

    # Given pandas dataframe, it will return a spark's dataframe.
    def pandas_to_spark(self, pandas_df):
        columns = list(pandas_df.columns)
        types = list(pandas_df.dtypes)
        struct_list = []
        for column, typo in zip(columns, types): 
            struct_list.append(self.define_structure(column, typo))
        p_schema = StructType(struct_list)
        return sqlContext.createDataFrame(pandas_df, p_schema)
    
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

            for record in chunk:
                chunks.append(record)
        self.debug(
                f"{len(chunks)} chunks created")
        
        return chunks
    def compute_average(self, chunks=[], property='work'):
        filterBy = {
            'work': '0',
            'load': '1'
        }
        mapped = SparkSession\
            .builder\
            .appName("AvgWorkMapReduce")\
            .getOrCreate()
            # .createDataFrame(
            #     Row(
            #         id=int(x[0]),
            #         timestamp=x[1],
            #         value=float(x[2]),
            #         property=int(x[3]),
            #         plug_id=int(x[4]),
            #         household_id=int(x[5]),
            #         house_id=int(x[6])
            #     ) for x in chunks if x[3] == filterBy[property]).rdd.map(
            #     lambda row: (
            #         (row.plug_id, row.household_id, row.house_id), row.value
            #     )
            # )
        self.debug(
            f"trying to create DataFrame")
        pd_df = pd.DataFrame(chunks)
        spark_df = self.pandas_to_spark(pd_df)
        df = mapped.createDataFrame(spark_df)
        reduced = df.groupby(['house_id', 'household_id', 'plug_id']).agg(f.avg(f.when(df.property == 0, df.value)).alias('work'), f.avg(f.when(df.property == 1, df.value)).alias('load')).collect()
        # reduce = mapped.aggregateByKey(
        #     zeroValue=(0, 0),

        #     sum=lambda a, b: (a[0] + b, a[1] + 1),
        #     count=lambda a, b: (a[0] + b[0], a[1] + b[1]))\
        #     .mapValues(lambda x: x[0]/x[1]).collect()  # basic avg computation
        mapped.stop();
        return reduced

    def save_to_db(self, dbname, results):
        try:
            db = self.couchserver.create(dbname)
            self.debug(
                f"Successfully created new CouchDB database {dbname}")
        except:
            db = self.couchserver[dbname]
            self.debug(
                f"Successfully connected to existing CouchDB database {dbname}")
        self.debug(f'Preparing to save {len(results)} items to database')

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