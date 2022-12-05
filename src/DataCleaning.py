import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import *

# add more functions as necessary

def removePunctuation(df,col):
    df.withColumn(col, regexp_replace(r'[!"#$%\'()*+,-.:;<=>?@[\\]^_`{}~]', ''))
    return df

def info(df):
    print(df.columns)
    print(df.info())
    print(df.head())
    print(df.isnull().sum())
    print(df.duplicated().sum())


def latitude_longitude(df,col):  #by column
    df = df.withColumn(col, regexp_replace(regexp_replace(col, '\\(', ''),'\\)', ''))
    df = df.withColumn('latitude', split(df[col], ' ').getItem(1)).withColumn('longitude', split(df[col], ' ').getItem(2))
    df = df.drop(df[col])
    return df
    
def year_month(df,col):  #by column
    df = df.withColumn('year', split(df[col], '-').getItem(0)).withColumn('month', split(df[col], '-').getItem(1))
    return df

def naCheck(df,ifdrop):
    print(df.isnull().sum())
    if ifdrop:
        df.dropna(inplace=True)
        
def getCounty():
    pass
            
def sale_loader(inputs):
    raw = spark.read.options(header='True', inferSchema='True', delimiter=',').csv(inputs)
    raw.createOrReplaceTempView('sales')
    # truncate store and product info leaves ids only
    # correct the datatype
    sales = spark.sql("""
        select INT(`Bottles Sold`),Date(Date),STRING(`Invoice/Item Number`),DOUBLE(`Sale (Dollars)`),STRING(`Store Number`),STRING(`Item Number`) from sales --limit 100
    """) #`Store Number`,`Item Number`
    sales.show(10)
    return sales
    #sales.write.partitionBy('Date').parquet(output, mode='overwrite')
    
def product_loader(inputs):
    raw = spark.read.options(header='True', inferSchema='True', delimiter=',').csv(inputs)
    raw.createOrReplaceTempView('product')
    product = spark.sql("""
        select STRING(`Item Number`),Date(`List Date`),Date(`Report Date`),STRING(`Category Name`),STRING(`Item Description`),STRING(Vendor),STRING(`Vendor Name`),DOUBLE(`Bottle Volume (ml)`),INT(Pack),INT(`Inner Pack`),INT(Age),STRING(Proof),STRING(UPC),STRING(SCC),DOUBLE(`State Bottle Cost`),DOUBLE(`State Case Cost`),DOUBLE(`State Bottle Retail`) from product --limit 100
    """)
    product.show(10)
    return product
    #product.write.partitionBy('Category Name').parquet(output, mode='overwrite')
    
    
def store_loader(inputs):
    raw = spark.read.options(header='True', inferSchema='True', delimiter=',').csv(inputs)
    raw.createOrReplaceTempView('store')
    store = spark.sql("""
        select STRING(Store),Date(`Report Date`),STRING(Name),STRING(`Store Status`),STRING(Address),STRING(City),STRING(State),STRING(`Zip Code`),STRING(`Store Address`) from store --limit 100
    """)
    store.show(10)
    return store
    #store.write.partitionBy('City').parquet(output, mode='overwrite')

def df_parquet_writer(df,key,output):
    df.show(10)
    print(df.schema)
    df.write.partitionBy(key).parquet(output, mode='overwrite')
    
def df_csv_writer(df,key,output):
    df.show(10)
    print(df.schema)
    df.write.option("header",true).csv(output, mode='overwrite')

def main(data,inputs,output):
    #show info
    #info(df)
    
    if data=='store':
        #store
    	df = store_loader(inputs)
    	df = latitude_longitude(df,'Store Address')
    	df = df.na.fill(0)
    	df_parquet_writer(df,'City',output)
    
    elif data=='product':
        #product
        df = product_loader(inputs)
        df = df.na.fill(0)
        df_parquet_writer(df,'Category Name',output)
    
    elif data=='sale':
        #sale
        df = sale_loader(inputs)
        df = df.na.fill(0)
        df_parquet_writer(df,'Date',output)
    else:
        print('First parameter show be [store|product|sale]')
        
if __name__ == '__main__':
    inputs = sys.argv[2] #rawdata
    output = sys.argv[3] #parquet file
    data = sys.argv[1] # type
    inputs = sys.argv[2] #rawdata
    output = sys.argv[3] #parquet file
    data = sys.argv[1] #parquet file
    spark = SparkSession.builder.appName('datacleaning code').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(data, inputs, output)

    main(data,inputs, output)