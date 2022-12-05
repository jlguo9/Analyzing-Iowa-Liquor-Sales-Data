import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import datetime

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('total sales').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '2.4' # make sure we have Spark 2.4+


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
    product = spark.read.parquet(inputs+"/product") \
        .select('Item Number', 'Category Name', 'State Bottle Cost', 'Bottle Volume (ml)') \
        .cache()
    
    # divide the product into different bottle size
    size_max = product.agg(functions.max(product['Bottle Volume (ml)']).alias('max')).first()['max']
    size_min = product.agg(functions.min(product['Bottle Volume (ml)']).alias('min')).first()['min']
    size_divide_range = (size_max - size_min) / 3

    df_size = product.withColum("Bottle Size", divide(product['Bottle Volume (ml)'],
                'size', size_min+size_divide_range, size_max-size_divide_range)) \
            .select('Item Number', 'Category Name', 'Bottle Size')

    # divide the product into low-, mid-, and high-end
    price_per_lit = product.withColum("price_per_lit", product['State Bottle Cost'] / product['Bottle Volume (ml)']) \
        .select('Item Number','price_per_lit').cache()
    
    price_max = price_per_lit.agg(functions.max(price_per_lit['price_per_lit']).alias('max')).first()['max']
    price_min = price_per_lit.agg(functions.min(price_per_lit['price_per_lit']).alias('min')).first()['min']
    price_divide_range = (price_max - price_min) / 3

    df_graded = price_per_lit.withColum("Grade", divide(price_per_lit['price_per_lit'],
        'price', price_min+price_divide_range, price_max-price_divide_range)) \
        .select('Item Number','Grade')

    # join back together
    res_product = df_size.join(df_graded, 'Item Number') \
        .select('Item Number', 'Category Name', 'Bottle Size', 'Grade')
    return res_product
    

def load_sale(inputs):
    # load and extract features from sale data
    sale = spark.read.parquet(inputs+"/sale") \
        .select('Date', 'Sale (Dollars)', 'Store Number', 'Item Number')

    cur_year = datetime.date.today().year
    date_to_year = sale.withColumn('Year', functions.year(sale['Date'])) \
        .select('Year','Sale (Dollars)','Store Number','Item Number')
    selected = date_to_year.where((sale['Year'] >= cur_year-4) & (sale['Year'] <= cur_year-1) )

    store = spark.read.parquet(inputs+"/store") \
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