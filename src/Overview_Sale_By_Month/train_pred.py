import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('total sales').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '2.4' # make sure we have Spark 2.4+

from pyspark.ml.pipeline import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, SQLTransformer
from pyspark.ml.regression import GBTRegressor #GBTRegressor #DecisionTreeRegressor #RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import PipelineModel

my_schema = types.StructType([
    types.StructField('year', types.IntegerType()),
    types.StructField('month', types.IntegerType()),
    types.StructField('sale', types.DoubleType()),
])

def train(data, model_file):
    train, validation = data.randomSplit([0.75, 0.25])
    train = train.cache()
    validation = validation.cache()

    date_transformer = SQLTransformer(statement= \
    '''
    SELECT now.year as year, now.month as month, now.sale AS sale, last_year.sale AS last_year_sale
    FROM __THIS__ as now
    INNER JOIN __THIS__ as last_year
        ON now.year-1 = last_year.year
        AND now.month = last_year.month
    ''')
    feature_assembler = VectorAssembler(
        inputCols=['year','month','last_year_sale'],
        outputCol='feature')
    regressor = GBTRegressor(featuresCol="feature", labelCol='sale')

    pipeline = Pipeline(stages=[date_transformer, feature_assembler, regressor])
    model = pipeline.fit(train)

    # predict on validation set and evaluate
    predictions = model.transform(validation)
    # r^2
    r2_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='sale',
            metricName='r2')
    r2 = r2_evaluator.evaluate(predictions)
    print('r2 =', r2)
    
    # save the model
    model.write().overwrite().save(model_file)
    

def pred(data, model_file):
    column_names = ['year','month','sale']
    values = []
    for i in range(1, 13):
        values.append((2023,int(i),0))
    target = spark.createDataFrame(values, column_names)

    last_year = data.where(data['year']==2022)
    df = target.union(last_year)

    # load the model
    model = PipelineModel.load(model_file)
    
    # use the model to make prediction
    prediction = model.transform(df).select('year','month','prediction')
    return prediction

def main(inputs, model_file, outputs):
    sales_by_month = spark.read.csv(inputs, schema=my_schema)

    # train prediction model
    train(sales_by_month, model_file)

    # predict next year's sale for each month
    prediction = pred(sales_by_month, model_file)
    prediction.coalesce(1) \
        .write.option("header",True) \
        .csv(outputs, mode='overwrite')

if __name__ == '__main__':
    inputs = sys.argv[1]
    model_file = sys.argv[2]
    outputs = sys.argv[3]
    main(inputs, model_file, outputs)