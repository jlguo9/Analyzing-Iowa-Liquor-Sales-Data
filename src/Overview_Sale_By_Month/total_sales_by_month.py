import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import datetime
import os

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('total sales').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '2.4' # make sure we have Spark 2.4+

def main(inputs, outputs):
    data = spark.read.options(header='True', inferSchema='True', delimiter=',').csv(inputs) \
        .select(functions.col("Date").alias('date'), functions.col("Sale (Dollars)").alias('sale'))
    # extract year and month from date
    year_month = data.withColumn("year", functions.year(data['date'])) \
        .withColumn("month", functions.month(data['date'])) \
        .select('year', 'month', 'sale')
    sales_by_month = year_month.groupBy(['year','month']) \
        .agg(functions.sum(year_month['sale']).alias('sale')) \
        .orderBy(['year','month'], ascending=[1, 1]).cache()
    sales_by_month.coalesce(1) \
            .write.option("header",True) \
            .csv(outputs, mode='overwrite')

if __name__ == '__main__':
    inputs = sys.argv[1]
    outputs = sys.argv[2]
    main(inputs, outputs)