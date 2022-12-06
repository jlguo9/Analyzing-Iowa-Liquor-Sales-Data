import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
# from pyspark.sql.functions import col, percent_rank, lit
from datetime import datetime 

import matplotlib.pyplot as plot
# add more functions as necessary

def main(sales_inputs, product_inputs):
    # main logic starts here
    
    sales_data = spark.read.options(header='True', inferSchema='True', delimiter=',').parquet(sales_inputs)
    product_data = spark.read.options(header='True', inferSchema='True', delimiter=',').csv(product_inputs)
    #sales_data.show()
    product_data.show()
    #print(data.summary())


    product_data = product_data.withColumnRenamed('Item Number', 'Item Number_1')
    joined_data = sales_data.join(product_data,
        sales_data['Item Number'] == product_data['Item Number_1']
    )

    
    # .lt(functions.lit("2015-03-14")))
    #<=datetime.strptime("2022-10-30", "%Y-%m-%d") & joined_data['Date']>=datetime.strptime("2022-10-01", "%Y-%m-%d"))
    # joined_data.show()
    #datetime.strptime("2022-10-30", "%Y-%m-%d")
    

    joined_data = joined_data.select(joined_data['Item Number'],joined_data['Item Description'], joined_data['Date'], joined_data['Sale (Dollars)'], joined_data['Store Number'], joined_data['Bottles Sold'],joined_data['State Bottle Cost'])
    joined_data = joined_data.filter(joined_data['Date']<='2022-10-30')
    joined_data = joined_data.filter(joined_data['Date']>='2022-07-30')
    joined_data = joined_data.sort(joined_data['Date'], ascending=False)
    joined_data.show()


    # TODO: update sales with profit(i.e sales-retail)
    joined_data = joined_data.withColumn('Profit',  joined_data['Sale (Dollars)']-(joined_data['State Bottle Cost']*joined_data['Bottles Sold']))
    data = joined_data.groupby(joined_data['Item Description']).agg(functions.sum(joined_data['Bottles Sold']).alias('Total Bottles Sold'), functions.sum(joined_data['Sale (Dollars)']).alias('Total Sales'), functions.avg(joined_data['Profit']).alias('Avg Profit Per Order'))
    
    # data_2 = data.withColumn('Profit per Bottle',  data["Total Sales"]/data["Total Bottles Sold"])
    most_sales = data.sort(data["Total Bottles Sold"], ascending=False)
    mostSales_with_most_profit = most_sales.sort(data["Avg Profit Per Order"], ascending=False)
    mostSales_with_most_profit.show()

    mostSales_with_most_profit_p = mostSales_with_most_profit.select(mostSales_with_most_profit['Item Description'], mostSales_with_most_profit['Avg Profit Per Order']).limit(5).toPandas()

    mostSales_with_most_profit_p.plot.bar(stacked=False, legend=False, figsize=(20,10), width=.5)
    #data.summary()
    plot.show(block=True)

    

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('q3').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)
