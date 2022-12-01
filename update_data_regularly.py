# make sure to install these packages before running:
# pip install pandas
# pip install sodapy

import pandas as pd
import json
from sodapy import Socrata
from datetime import datetime, timedelta
import time

# from kafka import KafkaProducer
import threading

# load_updates function loads new data from last_update_date
# returns True and the result if pandas df when there is new data; 
# returns False and a None when there is no new data
def load_updates(last_update_date):
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
    results = client.get("m3tr-qhgy", where = select_statement, limit=5)    # limit=5 is for debugging. Remeber to remove it.

    # Convert to pandas DataFrame
    results_df = pd.DataFrame.from_records(results)
    if not results_df.empty:
        print(results_df)   # for debugging
        return True, results_df
    else:
        return False, None

# A thread function to write the new data we get into Kafka
def send_data(producer, data):
    topic = 'updates'
    producer.send(topic, data.encode('UTF-8'))

def main(last_update_date):
    # initialize kafka producer
    # producer = KafkaProducer(bootstrap_servers=['node1.local:?', 'node2.local:?'])

    dt_now = datetime.now()
    next_check = datetime(dt_now.year, dt_now.month, dt_now.day) + timedelta(1) # time to do next check (next day's midnight)
    is_successful, res= load_updates(last_update_date)
    if is_successful:
        last_update_date = datetime(dt_now.year, dt_now.month, dt_now.day)
        # TODO: do something with the data
        # send_data(producer, res)
    
    while True:
        print("Next check will be done at", next_check) # for debugging
        time.sleep((next_check-datetime.now()).total_seconds())
        is_successful, res= load_updates(last_update_date)
        if is_successful:
            last_update_date = datetime(next_check.year, next_check.month, next_check.day)
            # TODO: do something with the data
            # send_data(producer, res)
        next_check = next_check + timedelta(1)  # update next check time
    
if __name__ == '__main__':
    last_update_date = datetime(year=2022,month=11,day=1,hour=0,minute=0,second=0)
    # execute main as a daemon process
    # server_thread = threading.Thread(target=main, args=(last_update_date,))
    # server_thread.setDaemon(True)
    # server_thread.start()
    main(last_update_date)