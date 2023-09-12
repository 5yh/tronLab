#节点数据就用之前整理好的hdfs://192.168.101.29:9000/zxf/final_features/part*.csv
#边的数据就用hdfs://192.168.101.29:9000/zxf/final_graph_edges/part*.csv,边已经去重
import os
os.environ["SPARK_HOME"] = "/home/lxl/syh/miniconda3/envs/newspark/lib/python3.11/site-packages/pyspark"
import findspark
findspark.init()
import findspark
#import pandas as pd
findspark.init()
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml import Pipeline
from graphframes import GraphFrame 
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
import os
import pyspark.mllib.evaluation as ev
import numpy as np
from pyspark.sql.functions import mean as sqlmean
from graphframes.lib import AggregateMessages as AM
from pyspark import SparkContext

if __name__=='__main__':
    spark_session = SparkSession \
    .builder \
    .appName("get_graph_features") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

    #注意,id就是address
    spark_session.sparkContext.setLogLevel("Error")
    sc = SparkContext.getOrCreate()     
    all_features_schema=StructType([
                                StructField('id',StringType(),True),
                                StructField('Total_transactions',IntegerType(),True),
                                StructField('Number_of_received_addresses',IntegerType(),True),
                                StructField('Number_of_sent_addresses',IntegerType(),True),
                                StructField('Creatd_Contracts',IntegerType(),True),
                                StructField('Min_value_received',FloatType(),True),
                                StructField('Max_value_received',FloatType(),True),
                                StructField('Avg_value_received',FloatType(),True),
                                StructField('Total_ether_sent_for_accounts',FloatType(),True),
                                StructField('Min_value_sent',FloatType(),True),
                                StructField('Max_value_sent',FloatType(),True),
                                StructField('Avg_value_sent',FloatType(),True),
                                StructField('Total_ether_received_for_accounts',FloatType(),True),
                                StructField('Min_value_sent_to_contracts',FloatType(),True),
                                StructField('Max_value_sent_to_contracts',FloatType(),True),
                                StructField('Avg_value_sent_to_contracts',FloatType(),True),
                                StructField('Total_ether_sent_to_contracts',FloatType(),True),
                                StructField('USDT(sent_to_contract)',FloatType(),True),
                                StructField('BNB(sent_to_contract)',FloatType(),True),
                                StructField('UNI(sent_to_contract)',FloatType(),True),
                                StructField('USDC(sent_to_contract)',FloatType(),True),
                                StructField('LINK(sent_to_contract)',FloatType(),True),
                                StructField('MATIC(sent_to_contract)',FloatType(),True),
                                StructField('HEX(sent_to_contract)',FloatType(),True),
                                StructField('BUSD(sent_to_contract)',FloatType(),True),
                                StructField('WBTC(sent_to_contract)',FloatType(),True),
                                StructField('ACA(sent_to_contract)',FloatType(),True),
                                StructField('AAVE(sent_to_contract)',FloatType(),True),
                                StructField('SAI(sent_to_contract)',FloatType(),True),
                                StructField('DAI(sent_to_contract)',FloatType(),True),
                                StructField('SHIB(sent_to_contract)',FloatType(),True),
                                StructField('HT(sent_to_contract)',FloatType(),True),
                                StructField('MKR(sent_to_contract)',FloatType(),True),
                                StructField('CRO(sent_to_contract)',FloatType(),True),
                                StructField('COMP(sent_to_contract)',FloatType(),True),
                                StructField('LEO(sent_to_contract)',FloatType(),True),
                                StructField('FEI(sent_to_contract)',FloatType(),True),
                                StructField('USDT(sent)',FloatType(),True),
                                StructField('BNB(sent)',FloatType(),True),
                                StructField('UNI(sent)',FloatType(),True),
                                StructField('USDC(sent)',FloatType(),True),
                                StructField('LINK(sent)',FloatType(),True),
                                StructField('MATIC(sent)',FloatType(),True),
                                StructField('HEX(sent)',FloatType(),True),
                                StructField('BUSD(sent)',FloatType(),True),
                                StructField('WBTC(sent)',FloatType(),True),
                                StructField('ACA(sent)',FloatType(),True),
                                StructField('AAVE(sent)',FloatType(),True),
                                StructField('SAI(sent)',FloatType(),True),
                                StructField('DAI(sent)',FloatType(),True),
                                StructField('SHIB(sent)',FloatType(),True),
                                StructField('HT(sent)',FloatType(),True),
                                StructField('MKR(sent)',FloatType(),True),
                                StructField('CRO(sent)',FloatType(),True),
                                StructField('COMP(sent)',FloatType(),True),
                                StructField('LEO(sent)',FloatType(),True),
                                StructField('FEI(sent)',FloatType(),True),
                                StructField('USDT(received)',FloatType(),True),
                                StructField('BNB(received)',FloatType(),True),
                                StructField('UNI(received)',FloatType(),True),
                                StructField('USDC(received)',FloatType(),True),
                                StructField('LINK(received)',FloatType(),True),
                                StructField('MATIC(received)',FloatType(),True),
                                StructField('HEX(received)',FloatType(),True),
                                StructField('BUSD(received)',FloatType(),True),
                                StructField('WBTC(received)',FloatType(),True),
                                StructField('ACA(received)',FloatType(),True),
                                StructField('AAVE(received)',FloatType(),True),
                                StructField('SAI(received)',FloatType(),True),
                                StructField('DAI(received)',FloatType(),True),
                                StructField('SHIB(received)',FloatType(),True),
                                StructField('HT(received)',FloatType(),True),
                                StructField('MKR(received)',FloatType(),True),
                                StructField('CRO(received)',FloatType(),True),
                                StructField('COMP(received)',FloatType(),True),
                                StructField('LEO(received)',FloatType(),True),
                                StructField('FEI(received)',FloatType(),True),
                                StructField('Avg_time_between_sent)',IntegerType(),True),
                                StructField('Avg_time_between_received',IntegerType(),True),
                                StructField('Time_between_first_last',IntegerType(),True),
                                StructField('ERC20_avg_time_sent',IntegerType(),True),
                                StructField('ERC20_avg_time_received',IntegerType(),True),
                                StructField('ERC20_avg_time_contracts',IntegerType(),True),
                                StructField('inDegree',IntegerType(),True),
                                StructField('outDegree',IntegerType(),True),
                                # StructField('USDT(balance)',FloatType(),True),
                                # StructField('BNB(balance)',FloatType(),True),
                                # StructField('UNI(balance)',FloatType(),True),
                                # StructField('USDC(balance)',FloatType(),True),
                                # StructField('LINK(balance)',FloatType(),True),
                                # StructField('MATIC(balance)',FloatType(),True),
                                # StructField('HEX(balance)',FloatType(),True),
                                # StructField('BUSD(balance)',FloatType(),True),
                                # StructField('WBTC(balance)',FloatType(),True),
                                # StructField('ACA(balance)',FloatType(),True),
                                # StructField('AAVE(balance)',FloatType(),True),
                                # StructField('SAI(balance)',FloatType(),True),
                                # StructField('DAI(balance)',FloatType(),True),
                                # StructField('SHIB(balance)',FloatType(),True),
                                # StructField('HT(balance)',FloatType(),True),
                                # StructField('MKR(balance)',FloatType(),True),
                                # StructField('CRO(balance)',FloatType(),True),
                                # StructField('COMP(balance)',FloatType(),True),
                                # StructField('LEO(balance)',FloatType(),True),
                                # StructField('FEI(balance)',FloatType(),True),
                                # StructField('ETH(balance)',FloatType(),True)
                                ])
    # all_features_data = spark_session.read.option("header",True).schema(all_features_schema).csv('hdfs://ns00/lr/day_data/1.7/final_features/part*.csv')
    allEasyFeatureLoc="file:///mnt/blockchain02/tronLabData/easy_features"
    all_features_data = spark_session.read.csv(allEasyFeatureLoc,header=True, inferSchema=True)
    print(all_features_data.head(1))
    #all_features_data = all_features_data.withColumnRenamed('address','id')
    #边数据,src就是from_address,dst就是to_address
    # ether_data_schema=StructType([
    #                         StructField('id',IntegerType(),True),
    #                         StructField('src',StringType(),True),
    #                         StructField('dst',StringType(),True)
    #                         ])
    #edges_data=spark_session.read.option("header", True).option("timestampFormat", "yyyy/MM/dd, HH:mm:ss").schema(ether_data_schema).csv('hdfs://ns00/lr/abcddd.csv')
    edges_data = spark_session.read.option("header",True).option('inferSchema',True).csv('file:///mnt/blockchain02/tronLabData/edgefile(idfromto)')
    edges_data = edges_data.select('id','from','to')
    edges_data = edges_data.withColumnRenamed('from','src').withColumnRenamed('to','dst')
    #edges_data = edges_data.withColumnRenamed('from','src').withColumnRenamed('to','dst')
    #edges_data_=edges_data.select(all_features_data.columns[1:16])   
    edges_data_=edges_data.select(['src','dst'])


    #建图，一次性跑完所有的特征会内存报错,所以20个20个特征的跑,
    features=all_features_data.columns
    vertices=all_features_data.select(features)  #节点信息
    print('开始建图')
    graph = GraphFrame(vertices,edges_data_)  #边数据
    print('建图完毕')

   
    address_data=all_features_data.select('id')
    
    for col_ in features[1:]:       
        msgToSrc = AM.dst[col_]
        msgToDst = AM.src[col_]
        agg = graph.aggregateMessages(
            sqlmean(AM.msg).alias('mean_'+str(col_)),
            sendToSrc=msgToSrc,
            sendToDst=msgToDst)
        address_data=address_data.join(agg,'id','left')
        print('指标'+str(col_)+'已经计算完毕')
    address_data.withColumnRenamed('id','address').fillna(0)#.write.option("header", True).csv('/lr/first_month_data/graph_edges_')

    
    
    all_features_data = all_features_data.fillna(0)
    
    final_features_data = all_features_data.join(address_data,on = 'id',how = 'left').fillna(0)
    final_features_data.withColumnRenamed('id','address')
    print('----特征数据处理完成，开始保存----')
    final_features_data.write.option('header',True).csv('file:///mnt/blockchain02/tronLabData/three_month_data')
    print('特征数据保存完成')
    spark_session.stop()
    
    
 
