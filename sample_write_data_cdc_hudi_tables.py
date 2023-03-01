try:

    import os
    import sys
    import uuid

    import pyspark
    from pyspark.sql import SparkSession
    from pyspark import SparkConf, SparkContext
    from pyspark.sql.functions import col, asc, desc
    from pyspark.sql.functions import col, to_timestamp, monotonically_increasing_id, to_date, when
    from pyspark.sql.functions import *
    from pyspark.sql.types import *
    from datetime import datetime
    from functools import reduce
    from faker import Faker


except Exception as e:
    pass
SUBMIT_ARGS = "--packages org.apache.hudi:hudi-spark3.3-bundle_2.12:0.13.0 pyspark-shell"
os.environ["PYSPARK_SUBMIT_ARGS"] = SUBMIT_ARGS
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder \
    .config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
    .config('className', 'org.apache.hudi') \
    .config('spark.sql.hive.convertMetastoreParquet', 'false') \
    .getOrCreate()



db_name = "hudidb"
table_name = "hudi_cdc_table"

recordkey = 'uuid'
path = f"file:///C:/tmp/{db_name}/{table_name}"
precombine = "date"
method = 'upsert'
table_type = "MERGE_ON_READ"  # COPY_ON_WRITE | MERGE_ON_READ

hudi_options = {
    'hoodie.table.name': table_name,
    'hoodie.datasource.write.recordkey.field': recordkey,
    'hoodie.datasource.write.table.name': table_name,
    'hoodie.datasource.write.operation': method,
    'hoodie.datasource.write.precombine.field': precombine,
    
    'hoodie.table.cdc.enabled':'true',
    'hoodie.table.cdc.supplemental.logging.mode': 'data_before_after',
    
}


spark_df = spark.createDataFrame(
    data=[
    (1, "insert 1",  111,  "2020-01-06 12:12:12"),
    (2, "insert 2",  22, "2020-01-06 12:12:12"),
], 
    schema=["uuid", "message", "precomb", "date"])

spark_df.write.format("hudi"). \
    options(**hudi_options). \
    mode("append"). \
    save(path)

df = spark. \
      read. \
      format("hudi"). \
      load(path)

df.select(['_hoodie_commit_time', 'uuid', 'message']).show()

spark_df = spark.createDataFrame(
    data=[
    (1, "update 1",  111,  "2020-02-06 12:12:44"),
    (3, "insert 3",  33, "2020-02-06 12:12:32"),
], 
    schema=["uuid", "message", "precomb", "date"])

spark_df.write.format("hudi"). \
    options(**hudi_options). \
    mode("append"). \
    save(path)

df = spark. \
      read. \
      format("hudi"). \
      load(path)

df.select(['_hoodie_commit_time', 'uuid', 'message']).show()
