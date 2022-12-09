from kafka import KafkaConsumer
import json
import pandas as pd
import time
import threading
import os
import sys

# from pyspark.sql import SparkSession, functions, types

def sale(topic,output):
    consumer = KafkaConsumer(
            bootstrap_servers=['node1.local:9092', 'node2.local:9092'],
            auto_offset_reset='earliest',
            value_deserializer=lambda m: json.loads(m.decode('UTF-8'))
        )
    consumer.subscribe([topic])
    t = threading.current_thread()
    lst = []
    while getattr(t, "do_run", True):
        for msg in consumer:
            lst.append(msg)
            data = pd.DataFrame.from_records(lst)
        data.to_csv("./sale_tmp.csv")
        cmd = "python3 DataCleaning.py sale ./sale_tmp.csv "+output
        os.system(cmd)
        time.sleep(60)


def store(topic):
    consumer = KafkaConsumer(
            bootstrap_servers=['node1.local:9092', 'node2.local:9092'],
            auto_offset_reset='earliest',
            value_deserializer=lambda m: json.loads(m.decode('UTF-8'))
        )
    consumer.subscribe([topic])
    t = threading.current_thread()
    lst = []
    while getattr(t, "do_run", True):
        for msg in consumer:
            lst.append(msg)
            data = pd.DataFrame.from_records(lst)
        data.to_csv("./store_tmp.csv")
        cmd = "python3 DataCleaning.py store ./store_tmp.csv "+output
        os.system(cmd)
        time.sleep(60)

def product(topic):
    consumer = KafkaConsumer(
            bootstrap_servers=['node1.local:9092', 'node2.local:9092'],
            auto_offset_reset='earliest',
            value_deserializer=lambda m: json.loads(m.decode('UTF-8'))
        )
    consumer.subscribe([topic])
    t = threading.current_thread()
    lst = []
    while getattr(t, "do_run", True):
        for msg in consumer:
            lst.append(msg)
            data = pd.DataFrame.from_records(lst)
        data.to_csv("./product_tmp.csv")
        cmd = "python3 DataCleaning.py product ./product_tmp.csv "+output
        os.system(cmd)
        time.sleep(60)

def main(output):
    sale_thread = threading.Thread(target=sale, args=("sale",output))
    store_thread = threading.Thread(target=store, args=("store",output))
    product_thread = threading.Thread(target=product, args=("product",output))

    sale_thread.start()
    store_thread.start()
    product_thread.start()

    # (for debugging) stop the threads
    time.sleep(60)
    sale_thread.do_run = False
    store_thread.do_run = False
    product_thread.do_run = False

if __name__ == '__main__':
    # spark = SparkSession.builder.appName('Apply Update').getOrCreate()
    # assert spark.version >= '3.0' # make sure we have Spark 3.0+
    # spark.sparkContext.setLogLevel('WARN')
    # sc = spark.sparkContext
    output = sys.argv[1]
    main(output)