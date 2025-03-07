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
# 计算createContract个数
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
    # all_data= spark_session.read.option("header", True).schema(ether_data_schema).csv('file:///mnt/blockchain02/tronLabData/*.csv')
    tron2022DataLoc="file:///mnt/blockchain02/tronLabData/parseData38004100/*.csv"
    fromAddressLoc="file:///mnt/blockchain02/tronLabData/from_address.csv"
    toAddressLoc="file:///mnt/blockchain02/tronLabData/to_address.csv"
    easyFeatureLoc="file:///mnt/blockchain02/tronLabData/easy_features"
    all_data = spark_session.read.csv(tron2022DataLoc,header=True, inferSchema=True)
    from_Address = spark_session.read.csv(fromAddressLoc,header=True, inferSchema=True)
    to_Address = spark_session.read.csv(toAddressLoc,header=True, inferSchema=True)
    print("all_data数量%d" %all_data.count())
    # createContract=all_data.filter("Avg_value_sent_to_contracts != '0'")
    # createContract.show()
    # print(createContract.head(5))
    # print("createContract数量%d" %createContract.count())
    # from1=all_data.filter("from=='0x32026001f466ea538c38922f4f53ee064fb952cd'")
    # to1=all_data.filter("to=='0x32026001f466ea538c38922f4f53ee064fb952cd'")
    # from1.show()
    # to1.show()
    # print(from1.count())
    # print(to1.count())
    tmp1=all_data.filter("transHash=='0x3c20eb1808f58012ac2ebeebda60c709ec3e757d28368ad1ca9151f8db0a6c4b'")
    tmp1.show()
