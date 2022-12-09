# make sure to install these packages before running:
# pip install pandas
# pip install sodapy

import json
from sodapy import Socrata
from datetime import datetime, timedelta
import time

from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
import threading

# load_updates function loads new data from last_update_date
# returns True and the result if pandas df when there is new data; 
# returns False and a None when there is no new data
def load_updates(last_update_date, site):
    # #Example authenticated client (needed for non-public datasets):
    # client = Socrata(data.iowa.gov,
    #                  MyAppToken,
    #                  username="user@example.com",
    #                  password="AFakePassword")
    # 
    # # Unauthenticated client only works with public data sets. 
    # use application token, and no username or password:
    client = Socrata("data.iowa.gov", "33rk0cfiqCAEfxGARGUVWpW3l")

    date = last_update_date.strftime('%Y-%m-%dT%H:%M:%S.000')   # convert datetime to string
    select_statement = ":created_at > '" + date + "' OR :updated_at > '" + date + "'"   # prepare the WHERE statement
    
    # "results" is a list of dictionary. Each dictionary is one row from the dataset
    results = client.get(site, where = select_statement)    # limit=5 is for debugging. Remeber to remove it.

    if results:
        # print(results[0])   # for debugging
        return True, results
    else:
        return False, None


# function to write the new data we get into Kafka
# for each dictionary in the list, convert it into json string, encode, and send
def send_data(producer, data, topic):
    for row in data:
        # Convert to json
        row_json = json.dumps(row)
        producer.send(topic, row_json.encode('UTF-8'))


# thread function to check sale updates regularly
def check_sale(producer, last_update_date):
    print("Start to check sale updates regularly")
    dt_now = datetime.now()
    next_check = datetime(dt_now.year, dt_now.month, dt_now.day) + timedelta(1) # time to do next check (next day's midnight)
    is_successful, res= load_updates(last_update_date, "m3tr-qhgy")
    if is_successful:
        last_update_date = datetime(dt_now.year, dt_now.month, dt_now.day) + timedelta(1) # to prevent always getting the same update in following checks
        send_data(producer, res, "sale")
    t = threading.current_thread()
    while getattr(t, "do_run", True):
        print("Next check will be done at", next_check) # for debugging
        time.sleep((next_check-datetime.now()).total_seconds())
        is_successful, res= load_updates(last_update_date)
        if is_successful:
            last_update_date = datetime(next_check.year, next_check.month, next_check.day) + timedelta(1)
            send_data(producer, res, "sale")
        next_check = next_check + timedelta(1)  # update next check time


# thread function to check store updates regularly
def check_store(producer, last_update_date):
    print("Start to check store updates regularly")
    dt_now = datetime.now()
    next_check = datetime(dt_now.year, dt_now.month, dt_now.day) + timedelta(1) # time to do next check (next day's midnight)
    is_successful, res= load_updates(last_update_date, "ykb6-ywnd")
    if is_successful:
        last_update_date = datetime(dt_now.year, dt_now.month, dt_now.day) + timedelta(1) # to prevent always getting the same update in following checks
        send_data(producer, res, "store")
    
    t = threading.current_thread()
    while getattr(t, "do_run", True):
        print("Next check will be done at", next_check) # for debugging
        time.sleep((next_check-datetime.now()).total_seconds())
        is_successful, res= load_updates(last_update_date)
        if is_successful:
            last_update_date = datetime(next_check.year, next_check.month, next_check.day) + timedelta(1)
            send_data(producer, res, "store")
        next_check = next_check + timedelta(1)  # update next check time


# thread function to check product updates regularly
def check_product(producer, last_update_date):
    print("Start to check product updates regularly")
    dt_now = datetime.now()
    next_check = datetime(dt_now.year, dt_now.month, dt_now.day) + timedelta(1) # time to do next check (next day's midnight)
    is_successful, res= load_updates(last_update_date, "gckp-fe7r")
    if is_successful:
        last_update_date = datetime(dt_now.year, dt_now.month, dt_now.day) + timedelta(1) # to prevent always getting the same update in following checks
        send_data(producer, res, "product")

    t = threading.current_thread()
    while getattr(t, "do_run", True):
        print("Next check will be done at", next_check) # for debugging
        time.sleep((next_check-datetime.now()).total_seconds())
        is_successful, res= load_updates(last_update_date)
        if is_successful:
            last_update_date = datetime(next_check.year, next_check.month, next_check.day) + timedelta(1)
            send_data(producer, res, "product")

        next_check = next_check + timedelta(1)  # update next check time


def main(last_update_date):
    try:
        admin = KafkaAdminClient(bootstrap_servers='localhost:9092')

        sale_topic = NewTopic(name='sale',
                        num_partitions=1,
                        replication_factor=1)
        store_topic = NewTopic(name='store',
                        num_partitions=1,
                        replication_factor=1)
        product_topic = NewTopic(name='product',
                        num_partitions=1,
                        replication_factor=1)
        admin.create_topics([sale_topic,store_topic,product_topic])
    except Exception:
        pass

    # initialize kafka producer
    producer = KafkaProducer(bootstrap_servers=['node1.local:9092', 'node2.local:9092'])

    sale_thread = threading.Thread(target=check_sale, args=(producer, last_update_date,))
    store_thread = threading.Thread(target=check_store, args=(producer, last_update_date,))
    product_thread = threading.Thread(target=check_product, args=(producer, last_update_date,))

    sale_thread.start()
    store_thread.start()
    product_thread.start()

    # (for debugging) stop the threads
    sale_thread.do_run = False
    store_thread.do_run = False
    product_thread.do_run = False

    
if __name__ == '__main__':
    last_update_date = datetime(year=2022,month=12,day=1,hour=0,minute=0,second=0)
    main(last_update_date)