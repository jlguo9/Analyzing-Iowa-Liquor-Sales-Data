import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import *
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, DoubleType
from pyspark.sql import Row


def main(input1,input2,output):
    df = spark.read.parquet(input1) #store
    df.show(5)
    df.printSchema()

    rfm = spark.read.option("header",True).csv(input2) #RFM
    #rfm = rfm.drop(rfm['index'])
    rfm.show(5)
    rfm.printSchema()

    res = rfm.join(df,rfm['Store Number'] ==  df['Store'],"left")
    res.show(truncate=False)
    res.write.option("header",True).csv(output, mode='overwrite')


if __name__ == '__main__':
    input1 = sys.argv[1] 
    input2 = sys.argv[2]
    output = sys.argv[3] 
    spark = SparkSession.builder.appName('Geo code').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs,output)