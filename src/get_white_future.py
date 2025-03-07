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
#作废
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
    ether_data_schema=StructType([
                            StructField('timestamp',IntegerType(),True),
                            StructField('from',StringType(),True),
                            StructField('to',StringType(),True),
                            StructField('coin',StringType(),True),
                            StructField('value',FloatType(),True),
                            StructField('transHash',StringType(),True),
                            StructField('gasUsed',FloatType(),True),
                            StructField('gaslimit',FloatType(),True),
                            StructField('fee',FloatType(),True),
                            StructField('fromType',StringType(),True),
                            StructField('toType',StringType(),True),
                            StructField('transType',StringType(),True),
                            StructField('isLoop',IntegerType(),True),
                            StructField('status',IntegerType(),True)
                            ])
    # all_data= spark_session.read.option("header", True).schema(ether_data_schema).csv('file:///mnt/blockchain02/tronLabData/*.csv')
    tron2022DataLoc="file:///mnt/blockchain02/tronLabData/*.csv"
    fromAddressLoc="file:///mnt/blockchain02/tronLabData/from_address.csv"
    toAddressLoc="file:///mnt/blockchain02/tronLabData/to_address.csv"
    all_data = spark_session.read.csv(tron2022DataLoc,header=True, inferSchema=True)
    # from_Address = spark_session.read.csv(fromAddressLoc,header=True, inferSchema=True)
    # to_Address = spark_session.read.csv(toAddressLoc,header=True, inferSchema=True)
    print(all_data.head(5))
    print(all_data.printSchema())
    # print(all_data.count())
    
    
    # train_data = all_data.filter(F.col("timestamp")>=1633017600)
    # train_data = train_data.filter(F.col("timestamp")<1609430400)
    # train_data_from = train_data.select('from')
    # train_data_to = train_data.select('to')
    # from_to_train_data = train_data_from.union(train_data_to).distinct()
    # test_data = all_data.filter(F.col("timestamp")>=1609430400)
    # test_data = test_data.filter(F.col("timestamp")<=1612108799)
    # test_data_from = test_data.select('from')
    # test_data_to = test_data.select('to')
    # test_data_from_to = test_data_from.union(test_data_to).distinct()
    # same_data = from_to_train_data.join(test_data_from_to,on = 'from',how = 'inner').withColumnRenamed('from',;'ddress')
    
    # all_data = all_data.filter(F.col("timestamp")>=1588262400)
    # all_data = all_data.filter(F.col("timestamp")<1588348800)
    
    # print(all_data.count())
    # print("alldata个数")
    # all_data_from = all_data.join(from_Address, "from", "inner")
    # all_data_to = all_data.join(to_Address, "to","inner")
    # print(all_data_from.count())
    # print(all_data_to.count())
    # all_data=all_data_from.union(all_data_to).distinct()
    # # print(all_data.head(5))
    # print(all_data.count())
    # ether_data_schema=StructType([
    #                         StructField('from',StringType(),True),
    #                         StructField('toType',StringType(),True),
    #                         StructField('transHash',StringType(),True),
    #                         StructField('fee',FloatType(),True),
    #                         StructField('isLoop',IntegerType(),True),
    #                         StructField('usePrice',FloatType(),True),
    #                         StructField('gaslimit',FloatType(),True),
    #                         StructField('gasUsed',FloatType(),True),
    #                         StructField('fromType',StringType(),True),
    #                         StructField('transType',StringType(),True),
    #                         StructField('rate',FloatType(),True),
    #                         StructField('rank',FloatType(),True),
    #                         StructField('to',StringType(),True),
    #                         StructField('id',FloatType(),True),
    #                         StructField('value',FloatType(),True),    
    #                         StructField('timestamp',IntegerType(),True),
    #                         StructField('coin',StringType(),True),
    #                         StructField('status',IntegerType(),True)
    #                         ])
    # all_data= spark_session.read.option("header", True).schema(ether_data_schema).csv('hdfs://ns00/lxl/2020_10_07.csv')
    # all_data = all_data.filter(F.col("isLoop")!=1)
    # # all_data = all_data.filter(F.col("timestamp")>1601395200)

    #将地址(from和to)还有合约地址全部转换为小写的
    all_data_lower=all_data.withColumn("coin",F.lower(all_data['coin'])) \
                            .withColumn("from",F.lower(all_data['from'])) \
                            .withColumn("to",F.lower(all_data['to']))
    
    #有些指标只用到了数据的from和to列,这些指标复用了dataframe,先将这些指标整理出来
    to_from_data=all_data_lower.select('from','to')
    
    #获取总地址,就是将from和to拼接在一起之后去重
    from_data=to_from_data.select('from')
    to_data=to_from_data.select('to')
    all_addresses=from_data.union(to_data).distinct().withColumnRenamed('from','address')
    
    #总交易次数,实际上就是from这一列和to这一列union之后再查一下各个的address出现的次数就可以了  
    final_data=from_data.union(to_data).withColumnRenamed('from', 'address')
    Total_transactions=final_data.groupBy(final_data['address']) \
                                 .count() \
                                 .withColumnRenamed('count', 'Total_transactions')
    
    #账户接收交易的地址总数,实际上只需要from和to的两列(复用to_from_data)，统计每个to接受了多少个from就可以
    Number_of_received_addresses=to_from_data \
                                             .groupBy(to_from_data['to']).count() \
                                             .withColumnRenamed('count', 'Number_of_received_addresses') \
                                             .withColumnRenamed('to', 'address')  
    
    ##账户发送交易的地址总数,还是只用到了from和to两列数据(复用to_from_data)
    #返回地址和对应的发送地址总数
    Number_of_sent_addresses=to_from_data \
                                         .groupBy(to_from_data['from']).count() \
                                         .withColumnRenamed('count', 'Number_of_sent_addresses') \
                                         .withColumnRenamed('from', 'address')  
    
    #建立合约的个数,也就是trandType中的createContract类型的交易
    contract_data=all_data_lower.select('from','transType').filter("transType == 'createContract'")
    Creatd_Contracts=contract_data.groupBy(contract_data['from']) \
                               .count().withColumnRenamed('count', 'Creatd_Contracts') \
                               .withColumnRenamed('from', 'address')
    
    #账户接受的最小金额,实际上就是统计to接受的最小金额,这里注意的使用的是transType中的ETH类型的数据
    value_data=all_data_lower.filter("transType == 'ETH'") \
                             .select('to','value')
    Min_value_received=value_data.groupBy(value_data['to']) \
                                 .min() \
                                 .withColumnRenamed('min(value)','Min_value_received') \
                                 .withColumnRenamed('to','address') 
    #账户接受的最大金额(这里复用上面的value_data)
    #value_data_max=all_data_lower.filter("transType == 'ETH'").select('to','value')
    Max_value_received=value_data.groupBy(value_data['to']) \
                                     .max() \
                                     .withColumnRenamed('max(value)','Max_value_received') \
                                     .withColumnRenamed('to','address') 
                                     
    #账户接受的平均金额
    #回地址和对应的接受的平均金额
    #value_data_avg=all_data_lower.filter("transType == 'ETH'").select('to','value')
    Avg_value_received=value_data.groupBy(value_data['to']) \
                                 .mean() \
                                 .withColumnRenamed('avg(value)','Avg_value_received') \
                                 .withColumnRenamed('to','address') 
    
    #账户接受的总金额(总以太币)这里依然复用上述的value_data
    #返回地址和对应的总以太币
    Total_ether_sent_for_accounts=value_data.groupBy(value_data['to']) \
                                            .sum() \
                                            .withColumnRenamed('sum(value)','Total_ether_sent_for_accounts') \
                                            .withColumnRenamed('to','address') 
    
    #账户发送的最小金额
    #返回地址和对应发送的最小金额
    value_data_from=all_data_lower.filter("transType == 'ETH'") \
                                  .select('from','value')
    Min_value_sent=value_data_from.groupBy(value_data_from['from']) \
                                  .min() \
                                  .withColumnRenamed('min(value)','Min_value_sent') \
                                  .withColumnRenamed('from','address') 

    #账户发送的最大金额,复用上面的value_data_from
    #返回地址和对应的发送的最大金额
    Max_value_sent=value_data_from.groupBy(value_data_from['from']) \
                                  .max() \
                                  .withColumnRenamed('max(value)','Max_value_sent') \
                                  .withColumnRenamed('from','address') 
                                  
    #账户发送的平均金额,复用上面的value_data_from
    #回地址和对应的接受的平均金额
    Avg_value_sent=value_data_from.groupBy(value_data_from['from']) \
                                  .mean() \
                                  .withColumnRenamed('avg(value)','Avg_value_sent') \
                                  .withColumnRenamed('from','address') 
                                  
    #账户发送的总金额(总以太币),复用上面的value_data_from
    #返回地址和对应的发送的总金额
    Total_ether_received_for_accounts=value_data_from.groupBy(value_data_from['from']) \
                                                     .sum() \
                                                     .withColumnRenamed('sum(value)','Total_ether_received_for_accounts') \
                                                     .withColumnRenamed('from','address') 
                                                     
    #向合约发送的最小金额,这里使用的数据类型是transType中的toContract,即直接向合约转账,转账币种是ETH
    value_data_contract=all_data_lower.filter("transType == 'toContract'") \
                                      .select('from','value')
    Min_value_sent_to_contracts=value_data_contract.groupBy(value_data_contract['from']) \
                                                   .min() \
                                                   .withColumnRenamed('min(value)','Min_value_sent_to_contracts') \
                                                   .withColumnRenamed('from','address') 
                                                   
    #向合约发送的最大金额,还是复用value_data_contract
    #返回地址和对应的最大金额
    Max_value_sent_to_contracts=value_data_contract.groupBy(value_data_contract['from']) \
                                                   .max() \
                                                   .withColumnRenamed('max(value)','Max_value_sent_to_contracts') \
                                                   .withColumnRenamed('from','address')  
                                                   
    #向合约发送的平均金额,复用value_data_contract
    #返回地址和对应的平均金额
    Avg_value_sent_to_contracts=value_data_contract.groupBy(value_data_contract['from']) \
                                                   .mean() \
                                                   .withColumnRenamed('avg(value)','Avg_value_sent_to_contracts') \
                                                   .withColumnRenamed('from','address') 
                                                   
    #向合约发送的总金额(总以太币)
    #返回地址和对应的发送的总金额
    Total_ether_sent_to_contracts=value_data_contract.groupBy(value_data_contract['from']) \
                                                     .sum() \
                                                     .withColumnRenamed('sum(value)','Total_ether_sent_to_contracts') \
                                                     .withColumnRenamed('from','address') 
    
    #将上述指标整合在一起,只想到了join这种很蠢的方法
    #整个过程需要12个小时吧,可惜
    final_easy_features_data=all_addresses.join(Total_transactions,on='address',how='left').fillna(0,subset=['Total_transactions']) \
                                          .join(Number_of_received_addresses,on='address',how='left').fillna(0,subset=['Number_of_received_addresses']) \
                                          .join(Number_of_sent_addresses,on='address',how='left').fillna(0,subset=['Number_of_sent_addresses']) \
                                          .join(Creatd_Contracts,on='address',how='left').fillna(0,subset=['Creatd_Contracts']) \
                                          .join(Min_value_received,on='address',how='left').fillna(0,subset=['Min_value_received']) \
                                          .join(Max_value_received,on='address',how='left').fillna(0,subset=['Max_value_received']) \
                                          .join(Avg_value_received,on='address',how='left').fillna(0,subset=['Avg_value_received']) \
                                          .join(Total_ether_sent_for_accounts,on='address',how='left').fillna(0,subset=['Total_ether_sent_for_accounts']) \
                                          .join(Min_value_sent,on='address',how='left').fillna(0,subset=['Min_value_sent']) \
                                          .join(Max_value_sent,on='address',how='left').fillna(0,subset=['Max_value_sent']) \
                                          .join(Avg_value_sent,on='address',how='left').fillna(0,subset=['Avg_value_sent']) \
                                          .join(Total_ether_received_for_accounts,on='address',how='left').fillna(0,subset=['Total_ether_received_for_accounts']) \
                                          .join(Min_value_sent_to_contracts,on='address',how='left').fillna(0,subset=['Min_value_sent_to_contracts']) \
                                          .join(Max_value_sent_to_contracts,on='address',how='left').fillna(0,subset=['Max_value_sent_to_contracts']) \
                                          .join(Avg_value_sent_to_contracts,on='address',how='left').fillna(0,subset=['Avg_value_sent_to_contracts']) \
                                          .join(Total_ether_sent_to_contracts,on='address',how='left').fillna(0,subset=['Total_ether_sent_to_contracts'])
                                          
    #final_easy_features_data = final_easy_features_data.join(same_data,on = 'address',how = 'inner')
    final_easy_features_data.write.option("header", True).csv('file:///mnt/blockchain02/tronLabData/all_easy_features')

    spark_session.stop()