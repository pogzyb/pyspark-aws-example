# bikeshare_ml_pipe.py
from pyspark.sql import SparkSession
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.pipeline import Pipeline
import pyspark.sql.functions as F
import argparse


###############################################################
# This script will:
# - be located on the EMR cluster (put there via the "setup" bootstrap action)
# - run on the EMR cluster (triggered via the submit_pyspark_job.py script)
# - read in all csv's that are found in "s3://<your-bucket-name>/bike-share-data/*" (--data argument)
# - create as well as drop features from the spark sql dataframe
# - perform cross validation and extract the best model for transforming the test or holdout dataset
# - create, fit, and save a "Pipeline" object to "s3://<your-bucket-name>/< --save argument >/< --name argument>"
###############################################################


def run_pipeline(name: str, data: str, save: str) -> None:

    spark = SparkSession.builder.appName(name).getOrCreate()
    df = spark.read.csv(data + '/*', header=True)
    print(f'The Entire Dataset has [{df.count()}] Rows.')

    # remove very short and very long rides
    five_minutes_as_seconds = 60 * 5
    three_hours_as_seconds = 60 * 60 * 3
    df = df.filter(df['Duration'] >= five_minutes_as_seconds).filter(df['Duration'] <= three_hours_as_seconds).count()

    # data transformations

    # - log1p of duration
    # - convert 'Start date' to a timestamp and extract: day-of-week, month, hour, minute
    # - drop columns:
    # 'Start date', 'End date', 'Start station', 'End station number', 'End station', 'Duration', 'Bike number'
    # - one hot encode 'Member type' and 'Start station number'

    df = df.withColumn('label', F.log1p(df.Duration))
    df = df.withColumn('Start date', F.to_timestamp('Start date', 'yyyy-MM-dd HH:mm:ss'))
    df = df.withColumn('day_of_week', F.dayofweek('Start date'))
    df = df.withColumn('week_of_year', F.weekofyear('Start date'))
    df = df.withColumn('month', F.month('Start date'))
    df = df.withColumn('minute', F.minute('Start date'))
    df = df.withColumn('hour', F.hour('Start date'))

    df = df.drop('Start date', 'End date', 'Start station', 'End station number',
                 'End station', 'Duration', 'Bike number')

    train, test = df.randomSplit([.7, .3])

    member_indexer = StringIndexer(inputCol='Member type', outputCol='member_idx')
    member_encoder = OneHotEncoder(inputCol='member_idx', outputCol='member_enc')
    station_indexer = StringIndexer(inputCol='Start station number', outputCol='station_idx')
    station_encoder = OneHotEncoder(inputCol='station_idx', outputCol='station_enc')

    vector = VectorAssembler(
        inputCols=['member_enc', 'station_enc', 'day_of_week', 'week_of_year', 'month', 'minute', 'hour'],
        outputCol='features'
    )

    rf = RandomForestRegressor()

    pipeline = Pipeline(
        stages=[
            member_indexer,
            member_encoder,
            station_indexer,
            station_encoder,
            vector,
            rf
        ]
    )

    evaluation = RegressionEvaluator()

    grid = ParamGridBuilder()
    grid = grid.addGrid(rf.maxDepth, [3, 5])
    grid = grid.addGrid(rf.numTrees, [20, 50])
    grid = grid.build()

    cv = CrossValidator(
        estimator=pipeline,
        estimatorParamMaps=grid,
        evaluator=evaluation,
        numFolds=5
    )
    print('\nDoing Cross Validation')
    cv_models = cv.fit(train)
    print(f'\nCV Results: {cv_models.avgMetrics} (RMSE)')
    best_model = cv_models.bestModel
    results = best_model.transform(test)
    print(f'\nResults on Holdout Dataset: {evaluation.evaluate(results)} (RMSE)')

    # print('\nRe-fitting Pipeline on entire Dataset')
    # cv_models = cv.fit(df)
    # print('\nSaving to Pipeline into S3')
    # entire_model.save('s3://api-collection-bucket/models/spark_pipe_v1')
    print('\nDone!')
    return


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--name', type=str, help='App or project name')
    parser.add_argument('--data', type=str, help='csv file location (e.g. s3://bucket/path/to/data)')
    parser.add_argument('--save', type=str, help='save location for spark pipeline object (e.g. s3://bucket/path/to)')
    args = parser.parse_args()

    run_pipeline(**args.__dict__)
