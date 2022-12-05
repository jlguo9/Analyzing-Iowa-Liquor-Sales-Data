import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

# add more functions as necessary

comments_schema = types.StructType([
    types.StructField('archived', types.BooleanType()),
    types.StructField('author', types.StringType()),
    types.StructField('author_flair_css_class', types.StringType()),
    types.StructField('author_flair_text', types.StringType()),
    types.StructField('body', types.StringType()),
    types.StructField('controversiality', types.LongType()),
    types.StructField('created_utc', types.StringType()),
    types.StructField('distinguished', types.StringType()),
    types.StructField('downs', types.LongType()),
    types.StructField('edited', types.StringType()),
    types.StructField('gilded', types.LongType()),
    types.StructField('id', types.StringType()),
    types.StructField('link_id', types.StringType()),
    types.StructField('name', types.StringType()),
    types.StructField('parent_id', types.StringType()),
    types.StructField('retrieved_on', types.LongType()),
    types.StructField('score', types.LongType()),
    types.StructField('score_hidden', types.BooleanType()),
    types.StructField('subreddit', types.StringType()),
    types.StructField('subreddit_id', types.StringType()),
    types.StructField('ups', types.LongType()),
    #types.StructField('year', types.IntegerType()),
    #types.StructField('month', types.IntegerType()),
])

def main(inputs, output):
    # main logic starts here
    
    data = spark.read.options(header='True', inferSchema='True', delimiter=',').parquet(inputs)
    #data.show()
    #print(data.summary())
    
    # TODO: update sales with profit(i.e sales-retail)
    data = data.groupby(data['Item Number']).agg(functions.sum(data['Bottles Sold']).alias('Total Bottles Sold'), functions.sum(data['Sale (Dollars)']).alias('Total Sales'))
    data_2 = data.withColumn('Profit per Bottle',  data["Total Sales"]/data["Total Bottles Sold"])
    most_sales = data_2.sort(data["Total Bottles Sold"], ascending=False)
    mostSales_with_most_profit = most_sales.sort(data["Total Sales"], ascending=False)
    mostSales_with_most_profit.show()
    #data.summary()
    # reddit_avg = reddits.groupby(reddits['subreddit']).agg(functions.avg(reddits['score']))
    # averages = reddit_avg.sort(reddits['subreddit'],ascending=True)   
    # #averages.explain()
    # averages.write.csv(output, mode='overwrite')
    

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('q3').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)
