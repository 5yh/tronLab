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
from pyspark.sql.functions import monotonically_increasing_id

if __name__=='__main__':
#############获取全部数据################################################
    spark_session = SparkSession \
    .builder \
    .appName("get_other_features") \
    .config("spark.some.config.option", "some-value") \
    .config("spark.driver.extraJavaOptions", "-Djava.io.tmpdir=/mnt/blockchain03/findFullData/tmpdata") \
    .config("spark.executor.extraJavaOptions", "-Djava.io.tmpdir=/mnt/blockchain03/findFullData/tmpdata") \
    .config("spark.driver.memory", "200g") \
    .config('spark.sql.shuffle.partitions',1000) \
    .getOrCreate()
    spark_session.sparkContext.setLogLevel("Error")
    tron2022DataLoc="file:///mnt/blockchain02/tronLabData/parseData38004100/*.csv"
    all_data = spark_session.read.csv(tron2022DataLoc,header=True, inferSchema=True)
    edge_data=all_data.select("from","to").distinct()
    edge_data = edge_data.withColumn("id", monotonically_increasing_id())
    edge_data.write.option('header',True).csv('file:///mnt/blockchain02/tronLabData/edgefile(idfromto)')
    edge_data.show()
