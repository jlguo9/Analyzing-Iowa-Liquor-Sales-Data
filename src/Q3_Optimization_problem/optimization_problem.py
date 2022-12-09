import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
# from pyspark.sql.functions import col, percent_rank, lit
from datetime import datetime 

import matplotlib.pyplot as plot
# add more functions as necessary


def plot_data(joined_data, start, end):
    joined_data = joined_data.filter(joined_data['Date']<=end)
    joined_data = joined_data.filter(joined_data['Date']>=start)
    joined_data = joined_data.sort(joined_data['Date'], ascending=False)
    joined_data = joined_data.withColumn('Profit',  joined_data['Sale (Dollars)']-(joined_data['State Bottle Cost']*joined_data['Bottles Sold']))
    data = joined_data.groupby(joined_data['Item Description']).agg(functions.sum(joined_data['Bottles Sold']).alias('Total Bottles Sold'), functions.sum(joined_data['Sale (Dollars)']).alias('Total Sales'), functions.sum(joined_data['Profit']).alias('Total Profit'))
    
    data = data.withColumn('Profit per Bottle',  data["Total Profit"]/data["Total Bottles Sold"])
    most_sales = data.sort(data["Total Bottles Sold"], ascending=False)
    mostSales_with_most_profit = most_sales.sort(data["Profit per Bottle"], ascending=False)
    #mostSales_with_most_profit.show()

    mostSales_with_most_profit_p = mostSales_with_most_profit.select(mostSales_with_most_profit['Item Description'], mostSales_with_most_profit['Profit per Bottle']).limit(5).toPandas()
    print(mostSales_with_most_profit_p)
    mostSales_with_most_profit_p.set_index('Item Description', inplace=True)
    #mostSales_with_most_profit_p.plot.bar(stacked=False, legend=False, figsize=(20,10), width=.5, y='Profit per Bottle')
    return mostSales_with_most_profit_p
    '''plot.ylabel("Profit per Bottle")
    plot.title(start + ' - ' + end)
    plot.show(block=True)'''


def main(sales_inputs, product_inputs):
    # main logic starts here
    
    sales_data = spark.read.options(header='True', inferSchema='True', delimiter=',').parquet(sales_inputs)
    product_data = spark.read.options(header='True', inferSchema='True', delimiter=',').csv(product_inputs)


    product_data = product_data.withColumnRenamed('Item Number', 'Item Number_1')
    joined_data = sales_data.join(product_data,
        sales_data['Item Number'] == product_data['Item Number_1']
    )
    print(sales_data.columns)
    

    joined_data = joined_data.select(joined_data['Item Number'],joined_data['Item Description'], joined_data['Date'], joined_data['Sale (Dollars)'], joined_data['Store Number'], joined_data['Bottles Sold'],joined_data['State Bottle Cost'])
    
    # joined_data.show()
    dates = [{'start': '2022-07-30','end': '2022-10-30'},{'start': '2022-04-30','end': '2022-07-30'},{'start': '2022-01-30','end': '2022-04-30'}]
    fig, axis = plot.subplots(3)
    for i, date in enumerate(dates):
        #print(date['start'])
        print(i)
        ax = plot_data(joined_data, date['start'], date['end'])
        #axis[i].ylabel("Profit per Bottle")
        axis[i].set_title(date['start'] + ' - ' + date['end'])
        
        #plot.show(block=True)
        axis[i].plot(ax['Item Description'], ax['Profit per Bottle'])
        
   
    

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('q3').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)
