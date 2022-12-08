import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import *
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, DoubleType
from pyspark.sql import Row
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import MinMaxScaler

#PySpark libraries
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.sql.functions import col, percent_rank, lit
from pyspark.sql.window import Window
from pyspark.sql import DataFrame, Row
from pyspark.sql.types import StructType
from functools import reduce  # For Python 3.x

from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator  # requires Spark 2.4 or later
import numpy as np
import time
import pandas as pd

import matplotlib.mlab as mlab
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator
from pyspark.sql import functions as F
# add more functions as necessary
def RScore(x):
    if  x <= 3:
        return 1
    elif x<= 5:
        return 2
    elif x<= 11:
        return 3
    else:
        return 4

def FScore(x):
    if  x <= 262:
        return 4
    elif x <= 560:
        return 3
    elif x <= 1204:
        return 2
    else:
        return 1

def MScore(x):
    if  x <= 31067:
        return 4
    elif x <= 56368:
        return 3
    elif x <= 152264:
        return 2
    else:
        return 1


def transData(data):
    return data.rdd.map(lambda r: [r[0],Vectors.dense(r[1:])]).toDF(['Store Number','rfm'])

def optimal_k(df_in,index_col,k_min, k_max,num_runs):
    '''
    Determine optimal number of clusters by using Silhoutte Score Analysis.
    :param df_in: the input dataframe
    :param index_col: the name of the index column
    :param k_min: the train dataset
    :param k_min: the minmum number of the clusters
    :param k_max: the maxmum number of the clusters
    :param num_runs: the number of runs for each fixed clusters

    :return k: optimal number of the clusters
    :return silh_lst: Silhouette score
    :return r_table: the running results table

    :author: Wenqiang Feng
    :email:  von198@gmail.com.com
    '''

    start = time.time()
    silh_lst = []
    k_lst = np.arange(k_min, k_max+1)

    r_table = df_in.select(index_col).toPandas()
    r_table = r_table.set_index(index_col)
    centers = pd.DataFrame()

    for k in k_lst:
        silh_val = []
        for run in np.arange(1, num_runs+1):

            # Trains a k-means model.
            kmeans = KMeans()\
                    .setK(k)\
                    .setSeed(int(np.random.randint(100, size=1)))
            model = kmeans.fit(df_in)

            # Make predictions
            predictions = model.transform(df_in)
            r_table['cluster_{k}_{run}'.format(k=k, run=run)]= predictions.select('prediction').toPandas()

            # Evaluate clustering by computing Silhouette score
            evaluator = ClusteringEvaluator()
            silhouette = evaluator.evaluate(predictions)
            silh_val.append(silhouette)

        silh_array=np.asanyarray(silh_val)
        silh_lst.append(silh_array.mean())

    elapsed =  time.time() - start

    silhouette = pd.DataFrame(list(zip(k_lst,silh_lst)),columns = ['k', 'silhouette'])

    print('+------------------------------------------------------------+')
    print("|         The finding optimal k phase took %8.0f s.       |" %(elapsed))
    print('+------------------------------------------------------------+')


    return k_lst[np.argmax(silh_lst, axis=0)], silhouette , r_table

R_udf = udf(lambda x: RScore(x), StringType())
F_udf = udf(lambda x: FScore(x), StringType())
M_udf = udf(lambda x: MScore(x), StringType())

def main(inputs,output):
    df = spark.read.parquet(inputs)
    df.show(5)
    df.printSchema()
    
    date_max = df.select(max('Date'))
    current = date_max.collect()
    print(current[0][0])
    # Calculatre Duration
    df = df.filter(df['Date']>'2021-12-31').withColumn('Duration', datediff(lit(current[0][0]), 'Date'))
    df.show(5)
    
    recency = df.groupBy('Store Number').agg(min('Duration').alias('Recency'))
    frequency = df.groupBy('Store Number', 'Invoice/Item Number').count()\
                        .groupBy('Store Number')\
                        .agg(count("*").alias("Frequency"))
    monetary = df.groupBy('Store Number').agg(round(sum('Sale (Dollars)'), 2).alias('Monetary'))
    rfm = recency.join(frequency,'Store Number', how = 'inner')\
             .join(monetary,'Store Number', how = 'inner')
    rfm.show(5)
    
    print(rfm.toPandas().describe())
    
    rfm_seg = rfm.withColumn("r_seg", R_udf("Recency"))
    rfm_seg = rfm_seg.withColumn("f_seg", F_udf("Frequency"))
    rfm_seg = rfm_seg.withColumn("m_seg", M_udf("Monetary"))
    rfm_seg.show(5)
    
    rfm_seg = rfm_seg.withColumn('RFMScore',
                             F.concat(F.col('r_seg'),F.col('f_seg'), F.col('m_seg')))
    rfm_seg.sort(F.col('RFMScore')).show(5)
    
    rfm_seg.groupBy('RFMScore')\
       .agg({'Recency':'mean',
             'Frequency': 'mean',
             'Monetary': 'mean'} )\
        .sort(F.col('RFMScore')).show(5)

    transformed= transData(rfm)
    transformed.show(5)

    scaler = MinMaxScaler(inputCol="rfm",\
         outputCol="features")
    scalerModel =  scaler.fit(transformed)
    scaledData = scalerModel.transform(transformed)
    scaledData.show(5,False)
    
    k, silh_lst, r_table = optimal_k(scaledData,'Store Number',2,10,20)
    
    spark.createDataFrame(silh_lst).show()
    
    k = 2
    kmeans = KMeans().setK(k).setSeed(1)
    model = kmeans.fit(scaledData)
    # Make predictions
    predictions = model.transform(scaledData)
    predictions.show(50,False)
    results = rfm_seg.join(predictions.select('Store Number','prediction'),'Store Number',how='left')
    results.show(5)
    
    results.toPandas().to_csv(output)
    
    
if __name__ == '__main__':
    inputs = sys.argv[1] #parquet
    output = sys.argv[2] #parquet
    spark = SparkSession.builder.appName('RFM code').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs,output)