import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
from datetime import datetime 

import matplotlib.pyplot as plot
# add more functions as necessary


def find_best_product_for_time_range(joined_data, start, end):
    joined_data = joined_data.filter(joined_data['Date']<=end)
    joined_data = joined_data.filter(joined_data['Date']>=start)
    joined_data = joined_data.sort(joined_data['Date'], ascending=False)
    joined_data = joined_data.withColumn('Profit',  joined_data['Sale (Dollars)']-(joined_data['State Bottle Cost']*joined_data['Bottles Sold']))
    data = joined_data.groupby(joined_data['Item Description']).agg(functions.sum(joined_data['Bottles Sold']).alias('Total Bottles Sold'), functions.sum(joined_data['Sale (Dollars)']).alias('Total Sales'), functions.sum(joined_data['Profit']).alias('Total Profit'))
    
    data = data.withColumn('Profit per Bottle',  data["Total Profit"]/data["Total Bottles Sold"])
    most_sales = data.sort(data["Total Bottles Sold"], ascending=False)
    mostSales_with_most_profit = most_sales.sort(data["Profit per Bottle"], ascending=False)

    mostSales_with_most_profit_p = mostSales_with_most_profit.select(mostSales_with_most_profit['Item Description'], mostSales_with_most_profit['Profit per Bottle']).limit(5).toPandas()
    print(mostSales_with_most_profit_p)
    mostSales_with_most_profit_p.set_index('Item Description', inplace=True)
    ax = mostSales_with_most_profit_p.plot.bar(stacked=False, legend=False, figsize=(20,10), width=.5, y='Profit per Bottle', rot=0)
    plot.ylabel("Profit per Bottle")
    plot.title(start + ' - ' + end)
    
    # matplotlib version >= '3.4.0' needed for below line
    # ax.bar_label(ax.containers[0])
    plot.savefig('Profit_per_Bottle_'+start + '-' + end + '.png')
    #plot.show(block=True)


def main(sales_inputs, product_inputs):
    # main logic starts here
    
    sales_data = spark.read.options(header='True', inferSchema='True', delimiter=',').parquet(sales_inputs)
    product_data = spark.read.options(header='True', inferSchema='True', delimiter=',').csv(product_inputs)


    product_data = product_data.withColumnRenamed('Item Number', 'Item Number_1')
    joined_data = sales_data.join(product_data,
        sales_data['Item Number'] == product_data['Item Number_1']
    )

    joined_data = joined_data.select(joined_data['Item Number'],joined_data['Item Description'], joined_data['Date'], joined_data['Sale (Dollars)'], joined_data['Store Number'], joined_data['Bottles Sold'],joined_data['State Bottle Cost'])
    
    dates = [{'start': '2022-07-30','end': '2022-10-30'},{'start': '2022-04-30','end': '2022-07-30'},{'start': '2022-01-30','end': '2022-04-30'}]
    for i, date in enumerate(dates):
        find_best_product_for_time_range(joined_data, date['start'], date['end'])
        
   
    

if __name__ == '__main__':
    sales_inputs = sys.argv[1]
    product_inputs = sys.argv[2]
    spark = SparkSession.builder.appName('Q3: Optimization Problem').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(sales_inputs, product_inputs)
