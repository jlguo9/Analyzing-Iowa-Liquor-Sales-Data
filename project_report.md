#<u> CMPT 732 Big Data Project Report</u>

## Problem definition: 
*What is the problem that you are trying to solve? What are the challenges of this problem?*


When any business owner decides to open a new store(or expanding an existing business) at new city or a new neighbourhood, It can be challenging for business owners to decide on the best location for their stores so that they can make more profits.
Not only that, a few more questions arise such as

- How are the sales of specific products of their domain?
- How much revenue does other business in same domian make?
- What products drives the most sales?
- What are the most popular products among different regions of the city they plan to expand or open their business in?
- Are there any seasonal changes in how products are consumed? 

Analysis on above questions is necessary to take decisions such as the optimal location for their business,  what products they will sell, and how to increase sales when starting a new or
expanding an existing business in a new city.


Therefore, by examining the past sales data, our
initiative intends to provide analysis on above mentioned questions to inform them with knowledge that can assist them in making profitable decisions.

Now, we plan to test the this strategy on the *["Liquor sales"](https://data.iowa.gov/Sales-Distribution/Iowa-Liquor-Sales/m3tr-qhgy)* dataset presented by Iowa Govt. 

Also, the above strategy can be extended to provide these analysis to stores in other domains.
For example, if Safeway plans to open a new store in a new city, we can provide them with answers to above questions and help them maximize their profits.


## Methodology: 
*What is the problem that you are trying to solve? Briefly explain which tool(s)/technique(s) were used for which task and why you chose to implement that way.*

As we mentioned, we used *["IOWA Liquor sales data"](https://data.iowa.gov/Sales-Distribution/Iowa-Liquor-Sales/m3tr-qhgy)* published by IOWA Government.
Now, after obtaining the data, the next task was to clean it, extract-transform-load it into a Data Warehouse and then eventually into a Data Mart, and then generate reports like grouping the most popular drink by region. And the final stage i.e. to visualize the data and make it presentable to the clients.

 1. **Brief on Data**

    The Iowa Alcoholic Beverages Division receives reports on wholesale transactions, and they make the data (from 2012 to the present) accessible at: [https://data.iowa.gov/](https://data.iowa.gov/Sales-Distribution/Iowa-Liquor-Sales/m3tr-qhgy).
The collection is around 4.5 GB in size and contains 19 million transactions from 2012 to the present. 


![Alt text](./project_design.png)


## Problems: 
*What problems did you encounter while attacking the problem? How did you solve them?*

- ###Analysing the Big Table
    One of the fist issues we faced was loading and analysing the main table present in the database. As it contained only one table that too with 23 columns, it was a little difficult to fully understand the data before we perform any sort of analysis on it.

- ###Dealing with New Data
   In this project we are analysing the old historic data to analyse the past trends regarding the sales of the product. But what about the new data which keeps getting generated everyday. Let's say we run this analysis after a week we might want to consider the last week's data as well in that analysis and for that we cannot clear and reload the whole database. Therefore, we added a script for Kafka streaming which would keep checking the data-set website every night and if it finds a new range of data it streams the data using **Kafka** and updates/appends our database with new rows, which can be used in next analysis.


## Results: 
*What are the outcomes of the project? What did you learn from the data analysis? What did you learn from the implementation?*


## [Project Summary](https://coursys.sfu.ca/2022fa-cmpt-732-g1/pages/ProjectSummary): 
*A summary of what you did to guide our marking.*


- Getting the data: Acquiring/gathering/downloading.
- ETL: Extract-Transform-Load work and cleaning the data set.
- Problem: Work on defining problem itself and motivation for the analysis.
- Algorithmic work: Work on the algorithms needed to work with the data, including integrating data mining and machine learning techniques.
- Bigness/parallelization: Efficiency of the analysis on a cluster, and scalability to larger data sets.
-  UI: User interface to the results, possibly including web or data exploration frontends.
- Visualization: Visualization of analysis results.
- Technologies: New technologies learned as part of doing the project. 

