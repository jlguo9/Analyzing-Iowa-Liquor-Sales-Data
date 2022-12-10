# Data Cleaning

For the main data source is a 5.7 GB file iowa-liquor. Through analysis, we found that this table contains store information, product information and transaction information. Among them, there are a lot of redundant and wrong data in the store data, such as information loss, multiple names for the same store, etc. In addition, the geographical location is stored as coordinate point data instead of latitude and longitude, and there are errors in the date format. In order to facilitate analysis and remove redundant and erroneous data, we use the official store table and product table to replace the original data(by drop the columns from iowa-liquor). In this way, while proofreading the data, it also normalizes the data. After solving all the mentioned problems, the data becomes three tables of sale, store and product. Each table can be updated individually via Kafka in real-time. It is convenient for subsequent analysis and mining.

We performed this data cleaning action on Amazon EMR.

![](screenshot.png)
