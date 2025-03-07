import findspark
findspark.init()
from itertools import count
import sys
import time
import pandas as pd
import numpy as np
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *

if __name__=='__main__':
#############获取全部数据################################################
    spark_session = SparkSession \
    .builder \
    .appName("get_easy_features") \
    .config("spark.some.config.option", "some-value") \
    .config("spark.driver.extraJavaOptions", "-Djava.io.tmpdir=/mnt/blockchain03/findFullData/tmpdata") \
    .config("spark.executor.extraJavaOptions", "-Djava.io.tmpdir=/mnt/blockchain03/findFullData/tmpdata") \
    .config("spark.driver.memory", "200g") \
    .config('spark.sql.shuffle.partitions',1000) \
    .getOrCreate()
    spark_session.sparkContext.setLogLevel("Error")
    blackEasyFeatureBeforeDeleteNoBlackLoc="file:///mnt/blockchain02/tronLabData/easy_features"
    blackBeforeData = spark_session.read.csv(blackEasyFeatureBeforeDeleteNoBlackLoc,header=True, inferSchema=True)
    fromAddressLoc="file:///mnt/blockchain02/tronLabData/from_address.csv"
    toAddressLoc="file:///mnt/blockchain02/tronLabData/to_address.csv"
    from_Address = spark_session.read.csv(fromAddressLoc,header=True, inferSchema=True)
    to_Address = spark_session.read.csv(toAddressLoc,header=True, inferSchema=True)
    from_Address= from_Address.select("from").withColumnRenamed("from","address")
    to_Address= to_Address.select("to").withColumnRenamed("to","address")
    all_address=from_Address.union(to_Address).distinct()
    print(all_address.count())
    print(blackBeforeData.count())
    blackBeforeData=blackBeforeData.join(all_address, "address", "inner")
    print(blackBeforeData.count())
    # blackBeforeData.write.option("header", True).csv('file:///mnt/blockchain02/tronLabData/blackafterData')

