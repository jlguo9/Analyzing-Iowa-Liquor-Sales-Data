import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import datetime

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('total sales').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '2.4' # make sure we have Spark 2.4+

product_schema = types.StructType([
    types.StructField('Item Number', types.StringType()),
    types.StructField('Category Name', types.StringType()),
    types.StructField('Item Description', types.StringType()),
    types.StructField('Vendor', types.StringType()),
    types.StructField('Vendor Name', types.StringType()),
    types.StructField('Bottle Volume (ml)', types.DoubleType()),
    types.StructField('Pack', types.IntegerType()),
    types.StructField('Inner Pack', types.IntegerType()),
    types.StructField('Age', types.IntegerType()),
    types.StructField('Proof', types.StringType()),
    types.StructField('List Date', types.DateType()),
    types.StructField('UPC', types.StringType()),
    types.StructField('SCC', types.StringType()),
    types.StructField('State Bottle Cost', types.DoubleType()),
    types.StructField('State Case Cost', types.DoubleType()),
    types.StructField('State Bottle Retail', types.DoubleType()),
    types.StructField('Report Date', types.DateType()),
])

@functions.udf(returnType=types.StringType())
def divide(val, col, low, high):
    
    if col == 'price':
        if val < low:
            return 'low'
        elif val >= low and val < high:
            return 'mid'
        else:
            return 'high'
    if col == 'size':
        if val < low:
            return 's'
        elif val >= low and val < high:
            return 'm'
        else:
            return 'l'


def load_product(inputs):
    # load and extract features for product data
    product = spark.read.options(header='True').csv(inputs+"/product-datafixed.csv", schema=product_schema) \
        .select('Item Number', 'Category Name', 'State Bottle Cost', 'Bottle Volume (ml)') \
        .cache()
    # divide the product into different bottle size
    
    # size_max = product.agg(functions.max(product['Bottle Volume (ml)']).alias('max')).first()['max']
    # size_min = product.agg(functions.min(product['Bottle Volume (ml)']).alias('min')).first()['min']
    # size_divide_range = (size_max - size_min) / 3
    # Above method abandoned. Because bottle size are standarized, can simply put constants here
    df_size = product.withColumn("Bottle Size", divide(product['Bottle Volume (ml)'],
            functions.lit('size'), functions.lit(500), functions.lit(1000))) \
            .select('Item Number', 'Category Name', 'Bottle Size')

    # divide the product into low-, mid-, and high-end
    tmp = product.withColumn("price_per_lit", (product['State Bottle Cost'] / product['Bottle Volume (ml)']))
    price_per_lit = tmp.where(tmp["price_per_lit"].isNotNull()) \
            .select('Item Number','price_per_lit').cache()

    price_mean = price_per_lit.agg(functions.mean(price_per_lit['price_per_lit']).alias('mean')).first()['mean']
    price_var = price_per_lit.agg(functions.variance(price_per_lit['price_per_lit']).alias('var')).first()['var']

    df_graded = price_per_lit.withColumn("Grade", divide(price_per_lit['price_per_lit'],
        functions.lit('price'), functions.lit(price_mean-price_var/3), functions.lit(price_mean+price_var/3))) \
        .select('Item Number','Grade')

    # join back together
    res_product = df_size.join(df_graded, 'Item Number') \
        .select('Item Number', 'Category Name', 'Bottle Size', 'Grade')
    return res_product
    

def load_sale(inputs):
    # load and extract features from sale data
    sale = spark.read.options(header='True', inferSchema='True', delimiter=',').csv(inputs+"/sale-datafixed.csv") \
        .select('Date', 'Sale (Dollars)', 'Store Number', 'Item Number')

    cur_year = datetime.date.today().year
    date_to_year = sale.withColumn('Year', functions.year(sale['Date'])) \
        .select('Year','Sale (Dollars)','Store Number','Item Number')
    selected = date_to_year.where((sale['Year'] >= cur_year-4) & (sale['Year'] <= cur_year-1) )

    store = spark.read.options(header='True', inferSchema='True', delimiter=',').csv(inputs+"/store-datafixed.csv") \
        .select('Store', 'City')
    with_city = selected.join(store, selected['Store Number']==store['Store']) \
        .select(selected['Year'],selected['Sale (Dollars)'],selected['Item Number'],store['City'])
    
    return with_city


def main(inputs):
    product = load_product(inputs)
    sale = load_sale(inputs)
    # join into one df containing all features
    joined = sale.join(product, sale['Item Number']==product['Item Number']) \
        .select(sale['Year'],sale['Sale (Dollars)'],sale['City'],product['Category Name'], product['Bottle Size'], product['Grade']) \
        .cache()

    # TODO: create a list ['City','Category Name','Bottle Size','Grade'] and a nested loop of depth 2(maybe)
    # for each iteration:
    # - group by the corresponding feature, further group by Year to get sum(sale) of each year
    # - for each two consecutive year compute the growth rate, and take average
    # - then compute the variance and mean of the whole thing
    features = ['City','Category Name','Bottle Size','Grade']
    res_tag = []
    vars = []
    means = []
    
    # for single feature
    for i in features:
        res_tag.append(i)
        sub = joined.select('Year','Sale (Dollars)',i)
        by_year = sub.groupby(i,'Year').agg(functions.sum(sub['Sale (Dollars)']).alias('sale_by_year')).cache()
        by_year_2 = by_year
        with_last_year = by_year.join(by_year_2, by_year['Year']==by_year_2['Year']+1) \
            .select(by_year['Year'],by_year['Sale (Dollars)'],by_year[i],by_year_2['Sale (Dollars)'].alias('Last Year Sale'))
        growth = with_last_year.withColumn('Growth Rate', (with_last_year['Sale (Dollars)'] - with_last_year['Last Year Sale'])/with_last_year['Last Year Sale'])
        agg_growth = growth.groupby(i).agg(functions.mean(growth['Growth Rate']).alias('Avg Growth Rate'))

        var = agg_growth.agg(functions.variance(growth['Avg Growth Rate']).alias('var')).first()['var']
        mean = agg_growth.agg(functions.mean(growth['Avg Growth Rate']).alias('mean')).first()['mean']
        vars.append(var)
        means.append(mean)

    # for combinations of two features
    for i in features:
        for j in features:
            if i == j:
                continue
            res_tag.append(i+" + "+j)
            sub = joined.select('Year','Sale (Dollars)',i, j)
            by_year = sub.groupby(i, j,'Year').agg(functions.sum(sub['Sale (Dollars)']).alias('sale_by_year')).cache()
            by_year_2 = by_year
            with_last_year = by_year.join(by_year_2, by_year['Year']==by_year_2['Year']+1) \
                .select(by_year['Year'],by_year['Sale (Dollars)'],by_year[i],by_year[j],by_year_2['Sale (Dollars)'].alias('Last Year Sale'))
            growth = with_last_year.withColumn('Growth Rate', (with_last_year['Sale (Dollars)'] - with_last_year['Last Year Sale'])/with_last_year['Last Year Sale'])
            agg_growth = growth.groupby(i, j).agg(functions.mean(growth['Growth Rate']).alias('Avg Growth Rate'))
            
            var = agg_growth.agg(functions.variance(growth['Avg Growth Rate']).alias('var')).first()['var']
            mean = agg_growth.agg(functions.mean(growth['Avg Growth Rate']).alias('mean')).first()['mean']
            vars.append(var)
            means.append(mean)

    print(res_tag)
    print(vars)
    print(means)

if __name__ == '__main__':
    inputs = sys.argv[1]
    main(inputs)
