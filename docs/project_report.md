# <u> CMPT 732 Big Data Project Report</u>

## Problem definition: 
*What is the problem that you are trying to solve? What are the challenges of this problem?*


When any business owner decides to open a new store(or expanding an existing business) at new city or a new neighbourhood, It can be challenging for business owners to decide on the best location for their stores so that they can make more profits.
Not only that, a few more questions arise such as

- How are the sales of specific products of their domain?
- How much revenue does other business in same domian make?
- What products drives the most sales?
- What are the most popular products among different regions of the city they plan to expand or open their business in?
- Are there any seasonal changes in how products are consumed? 

*and more...*

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

    The initial(uncleaned) was around 5.7 GB in size and contains more than 25 million transactions from 2012 to the present along with 25 columns. 

    After performing Data Cleaning we were left with ~400mb of Data

2. **Some Questions we intend to answer**

    [**Question 1.**](https://github.sfu.ca/sna101/3_datamen_CMPT_732_project/tree/main/src/growth_rate#variance-of-growth-rate-under-different-market-segmentation)
    
    **(a)** Many features and their combinations(For example: cities, mid-to-high-end wines, wine categories） can have an impact on the sales growth rate. We would like to explore which of them have significant impact on sales so that people can know what they should take into consideration when they want to explore potential market segmentation.

    **(b)** Inverted display of data under significantly weighted features/combinations

    **Question 2.** 

    **(a)** Clustering based upon Geographical location i.e. either based on Gps coordinates or zipcodes.

    **(b)** Correlation analysis (coeff -1 ～ 1), to explore substitutes and complements (store level)

    [**Question 3.**](https://github.sfu.ca/sna101/3_datamen_CMPT_732_project/tree/main/src/Q3_Optimization_problem#optimizing-profit)

     Assuming that the capital at hand is certain, how can we try to maximize the profit? I.e.  finding Capital turnover rate /Wine inventory which means The more frequently you buy, the higher the capital turnover rate (Optimization problem)


#### Proposed Solution Design:
![Alt text](./project_design.png)


## Problems: 
*What problems did you encounter while attacking the problem? How did you solve them?*

- ### Analysing the Big Table
    One of the fist issues we faced was loading and analysing the main table present in the database. As it contained only one table that too with 23 columns, it was a little difficult to fully understand the data before we perform any sort of analysis on it.

- ### Dealing with New Data
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
      * We used the Github Pages to create this website to host our report and User interface to result of our analysis and Visualizations.
- Visualization: Visualization of analysis results.
      * We used Matplotlib python package to plot a lot of charts to demonstrate the result of our analysis, such as scatter plot, bar charts and line charts.Instances of these can be found in the report and "README.md" of each problem
- Technologies: New technologies learned as part of doing the project. 
    * A few technologies we learned and used while completing this project are Amazon S3, Python Spark, Amazon AWS EC2, Matplotlib, Github Pages, Kafka streamiing

