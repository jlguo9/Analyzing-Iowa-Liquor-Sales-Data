## check_update.py

This is the producer of Kafka.

    $ nohup ./src/Data_Collecting/check_update.py &

## apply_update.py

This is the consumer of Kafka

    $ nohup ./src/Data_Collecting/apply_update.py &

## total_sales_by_month.py
    
    $ spark-submit ./src/Overview_Sale_By_Month/total_sales_by_month.py <inputs> <output>

where "inputs" is the path of sales table in csv format, and "output" is the path of location where you want the outputs to be stored. The output is the aggregated sale data by month, which can be visualized and can also be fed into train_pref.py

## train_pred.py

    $ spark-submit ./src/Overview_Sale_By_Month/train_pred.py <inputs> <modelfile> <output>

where "inputs" is the output of total_sales_by_month.py, "modelfile" is the path of location where you want the model files to be stored, and "output" is the path of location where the predicted sales will be stored

## variance.py

    $ spark-submit ./src/Q1_Growth_Rate/variance.py <inputs> <output>

where "inputs" is the path where there are three csv files: iowa-liquor-datefixed.csv, store-datefixed.csv, product-datefixed.csv
and "outputs" is the path of location where the output files will be stored. The output includes: 
1. a folder named "growth_rate_tables"
under which growth rate of each segment are stored under a folder named by the corresponding scenario.
2. the final resultant DataFrame containing variance, mean, max, min of each scenario

