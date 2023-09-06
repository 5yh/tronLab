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
from pyspark.sql.functions import lit

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
    allEasyFeatureLoc="file:///mnt/blockchain02/tronLabData/easy_features"
    allEasyFeature = spark_session.read.csv(allEasyFeatureLoc,header=True, inferSchema=True)
    fromAddressLoc="file:///mnt/blockchain02/tronLabData/from_address.csv"
    toAddressLoc="file:///mnt/blockchain02/tronLabData/to_address.csv"
    from_Address = spark_session.read.csv(fromAddressLoc,header=True, inferSchema=True)
    to_Address = spark_session.read.csv(toAddressLoc,header=True, inferSchema=True)
    from_Address= from_Address.select("from").withColumnRenamed("from","address")
    to_Address= to_Address.select("to").withColumnRenamed("to","address")
    allBlackAddress=from_Address.union(to_Address).distinct()
    print(allBlackAddress.count())
    print(allEasyFeature.count())
    blackEasyFeature=allEasyFeature.join(allBlackAddress, "address", "inner")
    
    print(blackEasyFeature.count())
    whiteEasyFeature=allEasyFeature.subtract(blackEasyFeature)
    print("白取差集后%d" %whiteEasyFeature.count())
    
    whiteEasyFeature=whiteEasyFeature.sample(False,1.0,114514).limit(10*blackEasyFeature.count())
    blackEasyFeature=blackEasyFeature.withColumn("label", lit(1))
    whiteEasyFeature=whiteEasyFeature.withColumn("label",lit(0))
    print(whiteEasyFeature.count())
    blackEasyFeature.write.option("header", True).csv('file:///mnt/blockchain02/tronLabData/blackEasyFeature')
    whiteEasyFeature.write.option("header", True).csv('file:///mnt/blockchain02/tronLabData/whiteEasyFeature')

