import os
os.environ["SPARK_HOME"] = "/home/lxl/syh/miniconda3/envs/newspark/lib/python3.11/site-packages/pyspark"
import findspark
findspark.init()
from itertools import count
import sys
import time
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark.sql.functions import pandas_udf
from pyspark.sql.functions import PandasUDFType
from pyspark.sql.functions import unix_timestamp


if __name__=='__main__':
#############获取transType类型################################################
    spark_session = SparkSession \
    .builder \
    .appName("groupbyTransType") \
    .config("spark.some.config.option", "some-value") \
    .config("spark.driver.extraJavaOptions", "-Djava.io.tmpdir=/mnt/blockchain03/findFullData/tmpdata") \
    .config("spark.executor.extraJavaOptions", "-Djava.io.tmpdir=/mnt/blockchain03/findFullData/tmpdata") \
    .config("spark.driver.memory", "200g") \
    .config('spark.sql.shuffle.partitions',1000) \
    .getOrCreate()
    spark_session.sparkContext.setLogLevel("Error")
    # all_data= spark_session.read.option("header", True).schema(ether_data_schema).csv('file:///mnt/blockchain02/tronLabData/*.csv')
    tron2022DataLoc="file:///mnt/blockchain02/tronLabData/parseData38004100/*.csv"
    # fromAddressLoc="file:///mnt/blockchain02/tronLabData/from_address.csv"
    # toAddressLoc="file:///mnt/blockchain02/tronLabData/to_address.csv"
    # easyFeatureLoc="file:///mnt/blockchain02/tronLabData/easy_features"
    all_data = spark_session.read.csv(tron2022DataLoc,header=True, inferSchema=True)
    print("all_data数量%d" %all_data.count())
    all_data=all_data.groupBy("transType")
    result=all_data.count()
    result.show()
# +--------------+---------+                                                      
# |     transType|    count|
# +--------------+---------+
# |    toContract| 15669158|
# |           trx|226731283|
# |createContract|  2564469|
# |  callContract|153196639|
# |         TRC20|149390889|
# +--------------+---------+
