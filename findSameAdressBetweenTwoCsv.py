import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark.sql.functions import *
import os
from shutil import rmtree

if __name__=='__main__':
    spark_session = SparkSession \
    .builder \
    .appName("findAddressJiaoji") \
    .config("spark.driver.extraJavaOptions", "-Djava.io.tmpdir=/mnt/blockchain03/findFullData/tmpdata") \
    .config("spark.executor.extraJavaOptions", "-Djava.io.tmpdir=/mnt/blockchain03/findFullData/tmpdata") \
    .config("spark.driver.memory", "200g") \
    .config('spark.sql.shuffle.partitions',1000) \
    .getOrCreate()
    spark_session.sparkContext.setLogLevel("Error")
    # tmpLoc="file://"+fileSaveLoc+"labeled_accounts.csv"
    blackAddressLoc="file:///mnt/blockchain02/tronLab/123.csv"
    tron2022DataLoc="file:///mnt/blockchain02/tronLabData/*.csv"
    # rawFiveAccounts = spark_session.read.csv(tmpLoc, header=True, inferSchema=True)

    # 应用函数到每一行
    # print("niha0o")
    # rawFiveAccounts.foreach(rawEachAccount)
    tronData=spark_session.read.csv(tron2022DataLoc,header=True, inferSchema=True)
    blackAddress=spark_session.read.csv(blackAddressLoc,header=True, inferSchema=True)
    blackAddress=blackAddress.select("aaaa").withColumnRenamed("aaaa","from").distinct()
    # tronData.show()
    tronDataFrom=tronData.select("from")
    tronDataTo=tronData.select("to")
    # tronDataFrom.show()
    print(tronDataFrom.head(3))
    # blackAddress.show()
    fromjiao=tronDataFrom.intersect(blackAddress).distinct()
    print("afterfromjiao")
    # fromjiao.show()
    blackAddress.withColumnRenamed("from","to")
    tojiao=tronDataTo.intersect(blackAddress).distinct()
    print("aftertojiao")
    # tojiao.show()    
    # blackAddress.show()
    fromjiao.write.option("header", "true").csv("file:///mnt/blockchain02/tronLab/fromjiao.csv")
    tojiao.write.option("header", "true").csv("file:///mnt/blockchain02/tronLab/tojiao.csv")
    spark_session.stop()