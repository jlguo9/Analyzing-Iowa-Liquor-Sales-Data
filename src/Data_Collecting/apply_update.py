from kafka import KafkaConsumer
import json
import pandas as pd
import time
import threading
import os
import sys

# flush the data to intermediate csv and fed it into data cleanng pipeline
def data_action(lst, channel, output):
    if len(lst) == 0:
        return
    data = pd.DataFrame.from_records(lst)
    itermediate_file = "./"+channel+"_tmp.csv"
    data.to_csv(itermediate_file)
    cmd = "python3 DataCleaning.py "+channel+" "+itermediate_file+" "+output
    os.system(cmd)

def sale(topic,output):
    consumer = KafkaConsumer(
            bootstrap_servers=['node1.local:9092', 'node2.local:9092'],
            auto_offset_reset='earliest',
            value_deserializer=lambda m: json.loads(m.decode('UTF-8'))
        )
    consumer.subscribe([topic])
    print("sale consumer is on")

    # check stop signal
    t = threading.current_thread()
    while getattr(t, "do_run", True):
        lst = []
        start_time = time.time()
        for msg in consumer:
            lst.append(msg.value)
            cur_time = time.time()
            # flush the data every 5 minutes 
            if int(cur_time - start_time)==300 and len(lst) != 0:
                data_action(lst,"sale",output)
                start_time = cur_time
                lst.clear()
            # check stop signal
            if getattr(t, "do_run", False):
                break
        # flush the remaining data before close
        if len(lst) != 0:
            data_action(lst,"sale",output)
    consumer.close()
    print("sale consumer closed")


def store(topic,output):
    consumer = KafkaConsumer(
            bootstrap_servers=['node1.local:9092', 'node2.local:9092'],
            auto_offset_reset='earliest',
            value_deserializer=lambda m: json.loads(m.decode('UTF-8'))
        )
    consumer.subscribe([topic])
    print("store consumer is on")

    # check stop signal
    t = threading.current_thread()
    while getattr(t, "do_run", True):
        lst = []
        start_time = time.time()
        for msg in consumer:
            lst.append(msg.value)
            cur_time = time.time()
            # flush the data every 5 minutes 
            if int(cur_time - start_time)==300 and len(lst) != 0:
                data_action(lst,"store",output)
                start_time = cur_time
                lst.clear()
            # check stop signal
            if getattr(t, "do_run", False):
                break
        # flush the remaining data before close
        if len(lst) != 0:
            data_action(lst,"sale",output)
    consumer.close()
    print("store consumer closed")
    

def product(topic,output):
    consumer = KafkaConsumer(
            bootstrap_servers=['node1.local:9092', 'node2.local:9092'],
            auto_offset_reset='earliest',
            value_deserializer=lambda m: json.loads(m.decode('UTF-8'))
        )
    consumer.subscribe([topic])
    print("product consumer is on")

    # check stop signal
    t = threading.current_thread()
    while getattr(t, "do_run", True):
        lst = []
        start_time = time.time()
        for msg in consumer:
            lst.append(msg.value)
            cur_time = time.time()
            # flush the data every 5 minutes 
            if int(cur_time - start_time)==300 and len(lst) != 0:
                data_action(lst,"product",output)
                start_time = cur_time
                lst.clear()
            # check stop signal
            if getattr(t, "do_run", False):
                break
        # flush the remaining data before close
        if len(lst) != 0:
            data_action(lst,"sale",output)
    consumer.close()
    print("product consumer closed")

def main(output):
    sale_thread = threading.Thread(target=sale, args=("sale",output,))
    store_thread = threading.Thread(target=store, args=("store",output,))
    product_thread = threading.Thread(target=product, args=("product",output,))

    sale_thread.start()
    store_thread.start()
    product_thread.start()

    # (for debugging) stop the threads
    time.sleep(15)
    sale_thread.do_run = False
    store_thread.do_run = False
    product_thread.do_run = False

if __name__ == '__main__':
    output = sys.argv[1]
    main(output)