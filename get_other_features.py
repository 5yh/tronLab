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


##########################################################################################################
#下面的指标全部涉及到了时间,因此需要用一下pandas_udf,pandas_udf需要将数据全部缓存到内存当中,因此运行的时候最好多申请一些内存
#涉及到时间的总共有6个指标
#!!!!!!!!!!!!!!!!!!!!!!!!由于交易时间精确到了秒,所以指标的单位也是秒
#1.Avg_time_between_sent(账户发送交易的平均时间)
#2.Avg_time_between_received(账户接收交易的平均时间)
#3.Time_between_first_last(最初与最末一笔交易间的时差)
#4.ERC20_avg_time_sent(发送 ERC20 代币交易间的平均时间)
#5.ERC20_avg_time_received(接收 ERC20 代币交易间的平均时间)
#6.ERC20_avg_time_contracts(与合约进行 ERC20 代币交易的平均时间)
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
    # all_data = all_data.filter(F.col("timestamp")>=1601395200)
    # all_data = all_data.filter(F.col("timestamp")<1610035200)
    print("after read all_data")
    # all_data = all_data.withColumn("timestamp",F.to_timestamp(all_data['timestamp']))

    # ether_data_schema=StructType([
    #                         StructField('timestamp',TimestampType(),True),
    #                         StructField('from',StringType(),True),
    #                         StructField('to',StringType(),True),
    #                         StructField('coin',StringType(),True),
    #                         StructField('value',FloatType(),True),
    #                         StructField('transHash',StringType(),True),
    #                         StructField('gasUsed',FloatType(),True),
    #                         StructField('gaslimit',FloatType(),True),
    #                         StructField('fee',FloatType(),True),
    #                         StructField('fromType',StringType(),True),
    #                         StructField('toType',StringType(),True),
    #                         StructField('transType',StringType(),True),
    #                         StructField('isLoop',IntegerType(),True),
    #                         StructField('status',IntegerType(),True)
    #                         ])
    # all_data= spark_session.read.option("header", True).option("timestampFormat", "yyyy/MM/dd, HH:mm:ss").schema(ether_data_schema).csv('hdfs://ns00/lxl/new/erigon_parse_96000*.csv')
    # ether_data_schema=StructType([
    #                 StructField('from',StringType(),True),
    #                 StructField('toType',StringType(),True),
    #                 StructField('transHash',StringType(),True),
    #                 StructField('fee',FloatType(),True),
    #                 StructField('isLoop',IntegerType(),True),
    #                 StructField('usePrice',FloatType(),True),
    #                 StructField('gaslimit',FloatType(),True),
    #                 StructField('gasUsed',FloatType(),True),
    #                 StructField('fromType',StringType(),True),
    #                 StructField('transType',StringType(),True),
    #                 StructField('rate',FloatType(),True),
    #                 StructField('rank',FloatType(),True),
    #                 StructField('to',StringType(),True),
    #                 StructField('id',FloatType(),True),
    #                 StructField('value',FloatType(),True),    
    #                 StructField('timestamp',IntegerType(),True),
    #                 StructField('coin',StringType(),True),
    #                 StructField('status',IntegerType(),True)
    #                 ])
    # all_data= spark_session.read.option("header", True).schema(ether_data_schema).csv('hdfs://ns00/lxl/2020_10_07.csv')
    # all_data = all_data.filter(F.col("isLoop")!=1)
    # all_data = all_data.withColumn("timestamp",F.to_timestamp(all_data['timestamp']))
    all_data = all_data.withColumn("timestamp", unix_timestamp(all_data["timestamp"], "yyyy/MM/dd, HH:mm:ss"))
    all_data = all_data.withColumn("timestamp",F.to_timestamp(all_data['timestamp']))
    nm=all_data.select("timestamp")
    print(nm.head(5))
    # all_data.show(5)
    #还是先将from和to全部转换为小写格式
    all_data_lower=all_data.withColumn("coin",F.lower(all_data['coin'])) \
                           .withColumn("from",F.lower(all_data['from'])) \
                           .withColumn("to",F.lower(all_data['to']))
                          
    #Avg_time_between_sent(账户发送交易的平均时间)
    #这里注意的使用的是transType中的ETH类型和toContract的数据,之后再选取from和timastamp这两列数据
    #这里要注意的是,该指标求得是账户发送交易的平均时间,也就是说该账户只有发送了2笔以上的交易才可以求平均交易时间
    #因此当指标的结果为0的时候,意味着该账户只进行了一笔交易,即1笔交易没办法求交易的平均时间,用0代替
    value_data_from=all_data_lower.filter("transType == 'trx'") \
                             .select('from','timestamp')
    @pandas_udf('int', PandasUDFType.GROUPED_AGG)
    def avg_time_between_sent(x):
        if len(x)>=2:
            return x.sort_values().diff()[1:].apply(lambda x : x.days*86400+x.seconds).mean()
        else:
            return 0
    Avg_time_between_sent=value_data_from.groupby("from") \
                                         .agg(avg_time_between_sent(value_data_from['timestamp'])) \
                                         .withColumnRenamed('from','address') \
                                         .withColumnRenamed('avg_time_between_sent(timestamp)','Avg_time_between_sent')
    Avg_time_between_sent.count()
    Avg_time_between_sent.show(5)
    #Avg_time_between_received(账户接收交易的平均时间)
    #不过就是把from替换成了to而已
    value_data_to=all_data_lower.filter("transType == 'trx' ") \
                             .select('to','timestamp')
                             
    @pandas_udf('int', PandasUDFType.GROUPED_AGG)
    def avg_time_between_received(x):
        if len(x)>=2:
            return x.sort_values().diff()[1:].apply(lambda x : x.days*86400+x.seconds).mean()
        else:
            return 0
        
    Avg_time_between_received=value_data_to.groupby("to") \
                                           .agg(avg_time_between_received(value_data_to['timestamp'])) \
                                           .withColumnRenamed('to','address') \
                                           .withColumnRenamed('avg_time_between_received(timestamp)','Avg_time_between_received')
    
    
    #Time_between_first_last(最初与最末一笔交易间的时差)
    #这里是将账户作为发送方和接收方的交易时间纵向合并在一起之后再求时间差
    #            +------+-----+----------+         +-------+----------+
    #将这组数据   |from  |to   |timestamp |  变为   |address|timestamp |
    #            +------+-----+----------+         +-------+----------+
    #            |ab    |cd   |2012-12-31|         |ab     |2012-12-31|
    #            |LAKAER|ab   |2022-5-31 |         |ab     |2022-5-31 |
    #            +------+-----+----------+         |cd     |2012-12-31|
    #                                              |LAKER  |2022-5-31 |
    #                                              +-------+----------+
    #之后在对每一个地址求交易时间的差值
    #需要注意的是,依然需要保证该账户需要至少进行了两笔交易才可以得到该指标,因此该账户只有一笔交易的时候,交易差视为0
    value_data_from_to=value_data_from.union(value_data_to).withColumnRenamed('from','address')
    @pandas_udf('int', PandasUDFType.GROUPED_AGG)
    def time_between_first_last(x):
        if len(x)>=2:
            return (x.max()-x.min()).days*86400+(x.max()-x.min()).seconds#x.sort_values().diff()[1:].apply(lambda x : x.seconds).mean()
        else:
            return 0
    Time_between_first_last=value_data_from_to.groupby("address") \
                             .agg(time_between_first_last(value_data_from_to['timestamp'])) \
                             .withColumnRenamed('time_between_first_last(timestamp)','Time_between_first_last')
                             
    # Time_between_first_last.show()
    print(Time_between_first_last.head(5))
    
    #ERC20_avg_time_sent(发送 ERC20 代币交易间的平均时间)
    #也就是筛选出transType == 'ERC20'的数据就可以了
    #因为求得都是平均时间,因此就用上面那个求均值的就可以了
    ERC20_data_from=all_data_lower.filter("transType == 'TRC20'") \
                                  .select('from','timestamp')
    ERC20_avg_time_sent=ERC20_data_from.groupby("from") \
                                       .agg(avg_time_between_sent(ERC20_data_from['timestamp'])) \
                                       .withColumnRenamed('from','address') \
                                       .withColumnRenamed('avg_time_between_sent(timestamp)','ERC20_avg_time_sent')
                                       
    
    #ERC20_avg_time_received(接收 ERC20 代币交易间的平均时间)
    #就是把from改为to就可以了啊
    ERC20_data_to=all_data_lower.filter("transType == 'TRC20'") \
                                .select('to','timestamp')
    ERC20_avg_time_received=ERC20_data_to.groupby("to") \
                                       .agg(avg_time_between_sent(ERC20_data_to['timestamp'])) \
                                       .withColumnRenamed('to','address') \
                                       .withColumnRenamed('avg_time_between_sent(timestamp)','ERC20_avg_time_received')
                                       
    
    
    #ERC20_avg_time_contracts(与合约进行 ERC20 代币交易的平均时间)
    #这里要要注意的是需要分别取出1.fromtype为contract,to为normal  2.from为normal,totype为contract两种情况的数据
    
    #第一种情况,账户作为发送方和合约进行交易的情况
    ERC20_data_from_contract=all_data_lower.filter("transType == 'TRC20'") \
                                           .filter("toType == 'contract' and fromType == 'normal'") \
                                           .select('from','timestamp')
                                         
    #第二种情况,账户作为接收方和合约进行交易的情况
    ERC20_data_to_contract=all_data_lower.filter("transType == 'TRC20'") \
                                         .filter("toType == 'normal' and fromType == 'contract'") \
                                         .select('to','timestamp')
    
    ERC20_data_from_to_contract=ERC20_data_from_contract.union(ERC20_data_to_contract) 
                                                        
    ERC20_avg_time_contracts=ERC20_data_from_to_contract.groupby("from") \
                                                        .agg(avg_time_between_sent(ERC20_data_from_to_contract['timestamp'])) \
                                                        .withColumnRenamed('from','address') \
                                                        .withColumnRenamed('avg_time_between_sent(timestamp)','ERC20_avg_time_contracts')           
                                                        
                                                        
    
    to_from_data=all_data_lower.select('from','to')

    #获取总地址,就是将from和to拼接在一起之后去重
    from_data=to_from_data.select('from')
    to_data=to_from_data.select('to')
    all_addresses=from_data.union(to_data).distinct().withColumnRenamed('from','address')   
    print('----开始合并特征----')
    final_easy_features_data=all_addresses.join(Avg_time_between_sent,on='address',how='left').fillna(0,subset=['Avg_time_between_sent']) \
                                          .join(Avg_time_between_received,on='address',how='left').fillna(0,subset=['Avg_time_between_received']) \
                                          .join(Time_between_first_last,on='address',how='left').fillna(0,subset=['Time_between_first_last']) \
                                          .join(ERC20_avg_time_sent,on='address',how='left').fillna(0,subset=['ERC20_avg_time_sent']) \
                                          .join(ERC20_avg_time_received,on='address',how='left').fillna(0,subset=['ERC20_avg_time_received']) \
                                          .join(ERC20_avg_time_contracts,on='address',how='left').fillna(0,subset=['ERC20_avg_time_contracts'])                                    
    print('----开始存数据----')
    final_easy_features_data.write.option("header", True).csv('file:///mnt/blockchain02/tronLabData/other_features_V3')