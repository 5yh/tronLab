import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark.sql.functions import *
# from pyspark.sql.functions import *

from convertTronAddressToEvmAddress import convertTronAddresstoEvmAddress

if __name__=='__main__':
    spark_session = SparkSession \
    .builder \
    .appName("convertLabeledAddress") \
    .config("spark.driver.extraJavaOptions", "-Djava.io.tmpdir=/mnt/blockchain03/findFullData/tmpdata") \
    .config("spark.executor.extraJavaOptions", "-Djava.io.tmpdir=/mnt/blockchain03/findFullData/tmpdata") \
    .config("spark.driver.memory", "100g") \
    .getOrCreate()
    spark_session.sparkContext.setLogLevel("Error")
    blackAddressLoc="file:///mnt/blockchain02/tronLab/labeled_address.csv"
    blackAddress=spark_session.read.csv(blackAddressLoc,header=True, inferSchema=True)
    blackAddress=blackAddress.select("address")
    transform_udf = udf(convertTronAddresstoEvmAddress, StringType())
    blackAddressWithEvmAddress = blackAddress.withColumn("aaaa", transform_udf("address"))
    blackAddressWithEvmAddress.show()
    blackAddressWithEvmAddress.write.option("header", "true").csv("file:///mnt/blockchain02/tronLab/123.csv")





    spark_session.stop()