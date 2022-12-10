## Data Collecting

### Downloading the Raw Data

    $ nohup wget https://data.iowa.gov/api/views/ykb6-ywnd/rows.csv?accessType=DOWNLOAD -O store.csv &

    $ nohup wget https://data.iowa.gov/api/views/gckp-fe7r/rows.csv?accessType=DOWNLOAD -O product.csv &

    $ nohup wget https://data.iowa.gov/api/views/m3tr-qhgy/rows.csv?accessType=DOWNLOAD -O iowaliquor.csv &

### Fixing the Date column

    $ sed -E "s#([0-9]{2})/([0-9]{2})/([0-9]{4})#\3-\1-\2#" < iowaliquor.csv | tr -d '$' > iowa-liquor-datefixed.csv

    $ sed -E "s#([0-9]{2})/([0-9]{2})/([0-9]{4})#\3-\1-\2#" < store.csv | tr -d '$' > store-datefixed.csv

    $ sed -E "s#([0-9]{2})/([0-9]{2})/([0-9]{4})#\3-\1-\2#" < product.csv | tr -d '$' > product-datefixed1.csv

    $ sed -E "s#([0-9]{2})/([0-9]{2})/([0-9]{4})#\3-\1-\2#" < product-datefixed1.csv | tr -d '$' > product-datefixed.csv

### Running the Data Cleaning Script
    $ spark-submit ./src/Data_Collecting/dataclean.py <sale|product|store> <inputs> <output>
	
where "sale|product|store" is the path of sale or product or stroe table in csv format, and "output" is the path of location where you want the outputs to be stored. The output is the data file after cleaning.

### check_update.py

This is the producer of Kafka.

    $ nohup ./src/Data_Collecting/check_update.py &

### apply_update.py

This is the consumer of Kafka

    $ nohup ./src/Data_Collecting/apply_update.py <output> &
    
where "output" is the path of the location where the newly collected data will be updated to.
There is a timeout to stop the loop, which is for test/debugging use. Can be removed when put into use.

## Overview:

### total_sales_by_month.py
    
    $ spark-submit ./src/Overview_Sale_By_Month/total_sales_by_month.py <inputs> <output>

where "inputs" is the path of sales table in csv format, and "output" is the path of location where you want the outputs to be stored. The output is the aggregated sale data by month, which can be visualized and can also be fed into train_pref.py

### train_pred.py

    $ spark-submit ./src/Overview_Sale_By_Month/train_pred.py <inputs> <modelfile> <output>

where "inputs" is the output of total_sales_by_month.py, "modelfile" is the path of location where you want the model files to be stored, and "output" is the path of location where the predicted sales will be stored

## Q1:

### Running variance.py 

    $ spark-submit ./src/Q1_Growth_Rate/variance.py <inputs> <output>

where "inputs" is the path where there are three csv files: iowa-liquor-datefixed.csv, store-datefixed.csv, product-datefixed.csv
and "outputs" is the path of location where the output files will be stored. The output includes: 
1. a folder named "growth_rate_tables"
under which growth rate of each segment are stored under a folder named by the corresponding scenario.
2. the final resultant DataFrame containing variance, mean, max, min of each scenario

## Q2:
### RFM segmentation and cluster

    $ spark-submit ./src/Q2_RFM_Cluster/RFM.py <saleData file> <output>
	
where "saleData file" is the path of sale table in csv format, and "output" is the path of location where you want the outputs to be stored. The output is the store number with its RFM segmentation and score combined with the prediction cluster after using kmeans algorithm.

### Joining RFM with GEO data

    $ spark-submit ./src/Q2_RFM_Cluster/joinGeoRfm.py <storeData file> <RFM file> <output>
	
where "storeData file" is the path of store table in parquet format, "RFM file" is the path of RFM file in csv format, and "output" is the path of location where you want the outputs to be stored. The output is RFM file combine with the geographic coordinates.

### Draw scatterplot with map
    $ nohup ./src/Q2_RFM_Cluster/DrawMap.py &


## Q3: 

### Running optimization_problem.py 

    $ spark-submit ./src/Q3_Optimization_problem/optimization_problem.py <input_1> <input_2>../../../project_data/testsale ../../../project_data/product


where "input_1" is the path to (normalized) folder which contains sales data(in parquet files) and "input_2" is the path to folder which contains product data
