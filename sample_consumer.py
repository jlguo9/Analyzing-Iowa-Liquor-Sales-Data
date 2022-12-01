from kafka import KafkaConsumer
import json
import pandas as pd

from pyspark.sql import SparkSession, functions, types

def consume(topic):
    consumer = KafkaConsumer(
            topic, bootstrap_servers=['node1.local:?', 'node2.local:?'],   
            value_deserializer=lambda m: json.loads(m.decode('UTF-8'))
        )   # replace ? by port number
    lst = []
    for msg in consumer:
        lst.append(msg)
    data = pd.DataFrame.from_records(lst)
    print(data)
    # do something like transfer pd df into pyspark df and so on
    mySchema = {}
    df = spark.createDataFrame(data, schema=mySchema)
    ...


if __name__ == '__main__':
    topic = 'updates'
    spark = SparkSession.builder.appName('Read Stream').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    consume(topic)