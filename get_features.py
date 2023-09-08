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

# ether_data_schema=StructType([
#                             StructField('timestamp',TimestampType(),True),
#                             StructField('from',StringType(),True),
#                             StructField('to',StringType(),True),
#                             StructField('coin',StringType(),True),
#                             StructField('value',FloatType(),True),
#                             StructField('transHash',StringType(),True),
#                             StructField('gasUsed',FloatType(),True),
#                             StructField('gaslimit',FloatType(),True),
#                             StructField('fee',FloatType(),True),
#                             StructField('fromType',StringType(),True),
#                             StructField('toType',StringType(),True),
#                             StructField('transType',StringType(),True),
#                             StructField('isLoop',IntegerType(),True),
#                             StructField('status',IntegerType(),True)
#                             ])

#获取总地址,将from和to进行合并之后去重
def get_all_address(all_data):
    #all_data= spark_session.read.option("header", True).option("timestampFormat", "yyyy/MM/dd, HH:mm:ss").schema(ether_data_schema).csv('hdfs://192.168.101.29:9000/zxf/ether_data/parse_data/erigon_parse*.csv')
    from_data=all_data.select('from')
    to_data=all_data.select('to')
    all_addresses=from_data.union(to_data).distinct().withColumnRenamed('from','address')
    return all_addresses

#计算发送交易的总数量，实际上就是所有账户的出度，之前已经统计好出度和入度了，这里直接用了
def Sent_tx(spark_session):
    sent_tx_schema=StructType([StructField('address',StringType(),True),StructField('outDegree',IntegerType(),True)])
    sent_result = spark_session.read.schema(sent_tx_schema).csv('hdfs://ns00/lr/day_data/1.7/outDegree_feature/*.csv')
    return sent_result

#计算接受交易的总数量，实际上就是所有账户的入度
def Received_tx(spark_session):
    receive_tx_schema=StructType([StructField('address',StringType(),True),StructField('inDegree',IntegerType(),True)])
    receive_result = spark_session.read.schema(receive_tx_schema).csv('hdfs://ns00/lr/day_data/1.7/inDegree_feature/*.csv')
    return receive_result

#建立合约的个数,也就是trandType中的createContract类型的交易
#all_data就是所有的原始数据,返回的是address和对应的调用合约的次数
def create_contract_count(all_data):        
    contract_data=all_data.select('from','transType').filter("transType == 'createContract'")
    contract_data=contract_data.groupBy(contract_data['from']) \
                               .count().withColumnRenamed('count', 'Creatd_Contracts') \
                               .withColumnRenamed('from', 'address')
    return contract_data  

#总交易次数,实际上就是from这一列和to这一列union之后再查一下各个的address出现的次数就可以了   
#all_data是所有的原始数据，返回的是address和对应的交易总次数
def all_contract_count(all_data):
    from_data=all_data.select('from')
    to_data=all_data.select('to')
    final_data=from_data.union(to_data).withColumnRenamed('from', 'address')    
    final_data=final_data.groupBy(final_data['address']).count().withColumnRenamed('count', 'Total_transactions') 
    return final_data

#账户接收交易的地址总数,实际上只需要from和to的两列，统计每个to接受了多少个from就可以
#！！！！！这里又用到了from和to的两列，这里用的时候可以直接缓存from和to的两列数据，这里先使用all_data作为参数
#返回地址和对应的接受地址总数
def Number_of_received_addresses(all_data):
    result=all_data.select('to','from').distinct() \
                                      .groupBy(all_data['to']).count() \
                                      .withColumnRenamed('count', 'Number_of_received_addresses') \
                                      .withColumnRenamed('to', 'address')    
    return result  

#账户发送交易的地址总数,还是只用到了from和to两列数据
#返回地址和对应的发送地址总数
def Number_of_sent_addresses(all_data):
    result=all_data.select('from','to').distinct() \
                                      .groupBy(all_data['from']).count() \
                                      .withColumnRenamed('count', 'Number_of_sent_addresses') \
                                      .withColumnRenamed('from', 'address')    
    return result  

#账户接受的最小金额,实际上就是统计to接受的最小金额,这里注意的使用的是transType中的ETH类型的数据
#返回地址和对应的接受的最小金额
def Min_value_received(all_data):
    value_data=all_data.filter("transType == 'ETH'") \
                       .select('to','value')
    result=value_data.groupBy(value_data['to']).min().withColumnRenamed('min(value)','Min_value_received') \
                                                     .withColumnRenamed('to','address') 
    return result

#账户接受的最大金额
#返回地址和对应的接受的最大金额
def Max_value_received(all_data):
    value_data=all_data.filter("transType == 'ETH'") \
                       .select('to','value')
    result=value_data.groupBy(value_data['to']).max().withColumnRenamed('max(value)','Max_value_received') \
                                                     .withColumnRenamed('to','address') 
    return result

#账户接受的平均金额
#回地址和对应的接受的平均金额
def Avg_value_received(all_data):
    value_data=all_data.filter("transType == 'ETH'") \
                       .select('to','value')
    result=value_data.groupBy(value_data['to']).mean().withColumnRenamed('avg(value)','Avg_value_received') \
                                                     .withColumnRenamed('to','address') 
    return result

#账户接受的总金额(总以太币)
#返回地址和对应的总以太币
def Total_ether_sent_for_accounts(all_data):
    value_data=all_data.filter("transType == 'ETH'") \
                       .select('to','value')
    result=value_data.groupBy(value_data['to']).sum().withColumnRenamed('sum(value)','Total_ether_sent_for_accounts') \
                                                     .withColumnRenamed('to','address') 
    return result   

#账户发送的最小金额
#返回地址和对应发送的最小金额
def Min_value_sent(all_data):
    value_data=all_data.filter("transType == 'ETH'") \
                       .select('from','value')
    result=value_data.groupBy(value_data['from']).min().withColumnRenamed('min(value)','Min_value_sent') \
                                                     .withColumnRenamed('from','address') 
    return result

#账户发送的最大金额
#返回地址和对应的发送的最大金额
def Max_value_sent(all_data):
    value_data=all_data.filter("transType == 'ETH'") \
                       .select('from','value')
    result=value_data.groupBy(value_data['from']).max().withColumnRenamed('max(value)','Max_value_sent') \
                                                       .withColumnRenamed('from','address') 
    return result

#账户发送的平均金额
#回地址和对应的接受的平均金额
def Avg_value_sent(all_data):
    value_data=all_data.filter("transType == 'ETH'") \
                       .select('from','value')
    result=value_data.groupBy(value_data['from']).mean().withColumnRenamed('avg(value)','Avg_value_sent') \
                                                        .withColumnRenamed('from','address') 
    return result

#账户发送的总金额(总以太币)
#返回地址和对应的发送的总金额
def Total_ether_received_for_accounts(all_data):
    value_data=all_data.filter("transType == 'ETH'") \
                       .select('from','value')
    result=value_data.groupBy(value_data['from']).sum().withColumnRenamed('sum(value)','Total_ether_received_for_accounts') \
                                                       .withColumnRenamed('from','address') 
    return result


#向合约发送的最小金额,这里使用的数据类型是transType中的toContract,即直接向合约转账,转账币种是ETH
def Min_value_sent_to_contracts(all_data):
    value_data=all_data.filter("transType == 'toContract'") \
                       .select('from','value')
    result=value_data.groupBy(value_data['from']).min().withColumnRenamed('min(value)','Min_value_sent_to_contracts') \
                                                     .withColumnRenamed('from','address') 
    return result

#向合约发送的最大金额
#返回地址和对应的最大金额
def Max_value_sent_to_contracts(all_data):
    value_data=all_data.filter("transType == 'toContract'") \
                       .select('from','value')
    result=value_data.groupBy(value_data['from']).max().withColumnRenamed('max(value)','Max_value_sent_to_contracts') \
                                                     .withColumnRenamed('from','address') 
    return result   

#向合约发送的平均金额
#返回地址和对应的平均金额
def Avg_value_sent_to_contracts(all_data):
    value_data=all_data.filter("transType == 'toContract'") \
                       .select('from','value')
    result=value_data.groupBy(value_data['from']).mean().withColumnRenamed('avg(value)','Avg_value_sent_to_contracts') \
                                                     .withColumnRenamed('from','address') 
    return result

#向合约发送的总金额(总以太币)
#返回地址和对应的发送的总金额
def Total_ether_sent_to_contracts(all_data):
    value_data=all_data.filter("transType == 'toContract'") \
                       .select('from','value')
    result=value_data.groupBy(value_data['from']).sum().withColumnRenamed('sum(value)','Total_ether_sent_to_contracts') \
                                                       .withColumnRenamed('from','address') 
    return result



#注:以下的指标同一用该账户的20种ERC代币交易金额代替
#1.账户接收ERC20代币交易中最大交易额   2.账户接收ERC20代币交易中平均交易额
#3.账户发送ERC20代币交易中最小交易额   4.账户发送ERC20代币交易中最大交易额
#5.账户发送ERC20代币交易中平均交易额   
if __name__=='__main__':
    spark_session = SparkSession \
    .builder \
    .appName("get_features") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
    spark_session.sparkContext.setLogLevel("Error")
    contract_address=['0xdac17f958d2ee523a2206206994597c13d831ec7',
                  '0xb8c77482e45f1f44de1745f52c74426c631bdd52',
                  '0x1f9840a85d5af5bf1d1762f925bdaddc4201f984',
                  '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',
                  '0x514910771af9ca656af840dff83e8264ecf986ca',
                  '0x7d1afa7b718fb893db30a3abc0cfc608aacfebb0',
                  '0x2b591e99afe9f32eaa6214f7b7629768c40eeb39',
                  '0x4fabb145d64652a948d72533023f6e7a623c7c53',
                  '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599',
                  '0x63d958d765f5bd88efdbd8afd32445393b24907f',
                  '0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9',
                  '0x89d24a6b4ccb1b6faa2625fe562bdd9a23260359',
                  '0x6b175474e89094c44da98b954eedeac495271d0f',
                  '0x95ad61b0a150d79219dcf64e1e6cc01f0b64c4ce',
                  '0x6f259637dcd74c767781e37bc6133cd6a68aa161',
                  '0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2',
                  '0xa0b73e1ff0b80914ab6fe0444e65848c4c34450b',
                  '0xc00e94cb662c3520282e6f5717214004a7f26888',
                  '0x2af5d2ad76741191d15dfe7bf6ac92d4bd912ca3',
                  '0x956f47f50a910163d8bf957cf5846d573e7f87ca']
    coin_symbol=['USDT','BNB','UNI','USDC','LINK','MATIC','HEX','BUSD','WBTC','ACA','AAVE','SAI','DAI','SHIB','HT','MKR','CRO','COMP','LEO','FEI']
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
    # all_data= spark_session.read.option("header", True).schema(ether_data_schema).csv('hdfs://ns00/lxl/t_edge_id')
    tron2022DataLoc="file:///mnt/blockchain02/tronLabData/parseData38004100/*.csv"
    all_data = spark_session.read.csv(tron2022DataLoc,header=True, inferSchema=True)
    all_data=all_data.withColumn("coin",F.lower(all_data['coin'])) \
                            .withColumn("to",F.lower(all_data['to']))
    # ether_data_schema=StructType([
    #                     StructField('from',StringType(),True),
    #                     StructField('toType',StringType(),True),
    #                     StructField('transHash',StringType(),True),
    #                     StructField('fee',FloatType(),True),
    #                     StructField('isLoop',IntegerType(),True),
    #                     StructField('usePrice',FloatType(),True),
    #                     StructField('gaslimit',FloatType(),True),
    #                     StructField('gasUsed',FloatType(),True),
    #                     StructField('fromType',StringType(),True),
    #                     StructField('transType',StringType(),True),
    #                     StructField('rate',FloatType(),True),
    #                     StructField('rank',FloatType(),True),
    #                     StructField('to',StringType(),True),
    #                     StructField('id',FloatType(),True),
    #                     StructField('value',FloatType(),True),    
    #                     StructField('timestamp',IntegerType(),True),
    #                     StructField('coin',StringType(),True),
    #                     StructField('status',IntegerType(),True)
    #                     ])
    # all_data= spark_session.read.option("header", True).schema(ether_data_schema).csv('hdfs://ns00/lxl/2020_10_07.csv')
    # all_data = all_data.filter(F.col("isLoop")!=1)
    # all_data = all_data.filter(F.col("timestamp")>=1601395200)
    # all_data = all_data.filter(F.col("timestamp")<1610035200)
    print(all_data.count())
    # all_data = all_data.withColumnRenamed("_from","from")
    all_data.show(5)
    ERC20_coin_result=all_data.select('from','to','value','coin','toType').filter(F.col("coin").isin(contract_address)).filter("toType == 'contract'")
    ERC20_coin_result=ERC20_coin_result.replace(to_replace=contract_address,value=coin_symbol,subset=['coin']) 
                                        
    
    from_ERC20_coin_result = ERC20_coin_result.select('from','value','coin').groupBy(['from']) \
                                                                            .pivot('coin', coin_symbol) \
                                                                            .agg(F.sum('value')) \
                                                                            .fillna(0).withColumnRenamed('from','address') 
    from_ERC20_coin_result.write.csv('file:///mnt/blockchain02/tronLabData/from_contract_ERC20_coin_value')       

    ERC20_coin_value_data=all_data.select('from','value','coin').filter(F.col("coin").isin(contract_address))
    ERC20_coin_value_data=ERC20_coin_value_data.replace(to_replace=contract_address,value=coin_symbol,subset=['coin'])
    from_ERC20_coin_value_data=ERC20_coin_value_data.groupBy(['from']) \
                                                    .pivot('coin', coin_symbol) \
                                                    .agg(F.sum('value')) \
                                                    .fillna(0).withColumnRenamed('from','address') 
    from_ERC20_coin_value_data.write.csv('file:///mnt/blockchain02/tronLabData/from_ERC20_coin_value')
    ERC20_coin_value_data=all_data.select('to','value','coin').filter(F.col("coin").isin(contract_address))
    ERC20_coin_value_data=ERC20_coin_value_data.replace(to_replace=contract_address,value=coin_symbol,subset=['coin'])
    to_ERC20_coin_value_data=ERC20_coin_value_data.groupBy(['to']) \
                                                    .pivot('coin', coin_symbol) \
                                                    .agg(F.sum('value')) \
                                                    .fillna(0).withColumnRenamed('to','address') 
    to_ERC20_coin_value_data.write.csv('file:///mnt/blockchain02/tronLabData/to_ERC20_coin_value')
    spark_session.stop()
