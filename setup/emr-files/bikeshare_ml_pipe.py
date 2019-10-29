# bikeshare_ml_pipe.py
from pyspark.sql import SparkSession
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.pipeline import Pipeline
from pyspark.ml.feature import StandardScaler
from pyspark.sql.types import *
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


def extract_best_params(params: dict) -> dict:
    best_params_as_dict = {}
    for param, value in params.items():
        param_name = param.name
        best_params_as_dict[param_name] = value
    return best_params_as_dict


def run_pipeline(name: str, data: str, save: str) -> None:

    spark = SparkSession.builder.appName(name).getOrCreate()

    # Dataset Creation #

    # read bike ride history csv's
    df = spark.read.csv(f'{data}/rides/*', header=True)
    df = df.select(['Duration', 'Start date', 'Start station number', 'Member type'])
    df = df.withColumn('Start station number', df['Start station number'].cast(IntegerType()))
    print(f'The rides dataset has [{df.count()}] rows!')

    # read station information csv
    stations = spark.read.csv(f'{data}/stations/*', header=True)
    print(f'The stations dataset has {stations.count()} rows!')
    stations = stations.withColumnRenamed('LATITUDE', 'start_station_lat')
    stations = stations.withColumnRenamed('LONGITUDE', 'start_station_long')
    stations = stations.withColumn('Start station number', stations['TERMINAL_NUMBER'].cast(IntegerType()))
    stations = stations.select(['start_station_lat', 'start_station_long', 'Start station number'])

    # remove rides longer than 1.5 hours
    one_and_a_half_hours = 60 * 60 * 1.5
    df = df.filter(df['Duration'] <= one_and_a_half_hours)

    # remove unknown 'Member type's
    df = df.filter(~(df['Member type'] == 'Unknown'))

    # make label/target feature
    df = df.withColumn('label', F.log1p(df.Duration))

    # join on 'Start station number'
    print('Merging rides and stations dataframes!')
    df = df.join(stations, on='Start station number', how='left')
    df = df.withColumn('start_station_long', df['start_station_long'].cast(DoubleType()))
    df = df.withColumn('start_station_lat', df['start_station_lat'].cast(DoubleType()))

    # remove non-existent stations
    df = df.filter(~(df['Start station number'] == 31008) & ~(df['Start station number'] == 32051) & ~(
            df['Start station number'] == 32034))

    print(f'Complete rides and stations dataset has {df.count()} rows!')

    # Feature Transformations #
    print('Doing Feature Transformations!')

    # convert to datetime type
    df = df.withColumn('Start date', F.to_timestamp('Start date', 'yyyy-MM-dd HH:mm:ss'))
    df = df.withColumn('day_of_week', F.dayofweek('Start date'))
    df = df.withColumn('week_of_year', F.weekofyear('Start date'))
    df = df.withColumn('month', F.month('Start date'))
    df = df.withColumn('minute', F.minute('Start date'))
    df = df.withColumn('hour', F.hour('Start date'))

    # make time features cyclical
    pi = 3.141592653589793

    df = df.withColumn('sin_day_of_week', F.sin(2 * pi * df['day_of_week'] / 7))
    df = df.withColumn('sin_week_of_year', F.sin(2 * pi * df['week_of_year'] / 53))
    df = df.withColumn('sin_month', F.sin(2 * pi * (df['month'] - 1) / 12))
    df = df.withColumn('sin_minute', F.sin(2 * pi * df['minute'] / 60))
    df = df.withColumn('sin_hour', F.sin(2 * pi * df['hour'] / 24))

    df = df.withColumn('cos_day_of_week', F.cos(2 * pi * df['day_of_week'] / 7))
    df = df.withColumn('cos_week_of_year', F.cos(2 * pi * df['week_of_year'] / 53))
    df = df.withColumn('cos_month', F.cos(2 * pi * (df['month'] - 1) / 12))
    df = df.withColumn('cos_minute', F.cos(2 * pi * df['minute'] / 60))
    df = df.withColumn('cos_hour', F.cos(2 * pi * df['hour'] / 24))

    # drop unused columns
    drop_columns = [
        'Start date',
        'Start station number',
        'Duration',
        'day_of_week',
        'week_of_year',
        'month',
        'minute',
        'hour'
    ]
    df = df.drop(*drop_columns)

    # Model and Pipeline #

    # split training and test
    train, test = df.randomSplit([.7, .3])

    # encode categorical column 'Member type'
    member_indexer = StringIndexer(inputCol='Member type', outputCol='member_idx')
    member_encoder = OneHotEncoder(inputCol='member_idx', outputCol='member_enc')

    # create vector of features named 'features'
    vector = VectorAssembler(
        inputCols=[
            'member_enc',
            'start_station_lat',
            'start_station_long',
            'sin_day_of_week',
            'cos_day_of_week',
            'sin_week_of_year',
            'cos_week_of_year',
            'sin_month',
            'cos_month',
            'sin_minute',
            'cos_minute',
            'sin_hour',
            'cos_hour',
        ],
        outputCol='features'
    )

    # scale features
    scaler = StandardScaler(
        inputCol='features',
        outputCol='scaled_features'
    )

    # define RF model
    rf = RandomForestRegressor(
        featuresCol='scaled_features'
    )

    # create pipeline and fill in stages
    pipeline = Pipeline(
        stages=[
            member_indexer,
            member_encoder,
            vector,
            scaler,
            rf
        ]
    )

    # evaluation method
    evaluation = RegressionEvaluator()

    # best parameter search
    grid = ParamGridBuilder()
    grid = grid.addGrid(rf.maxDepth, [3, 5])
    grid = grid.addGrid(rf.numTrees, [100])
    grid = grid.build()

    # run cross validation
    cv = CrossValidator(
        estimator=pipeline,
        estimatorParamMaps=grid,
        evaluator=evaluation,
        numFolds=7
    )

    print('Doing Cross Validation')

    cv_models = cv.fit(train)
    print(f'CV results: {cv_models.avgMetrics} (RMSE)')

    best_model = cv_models.bestModel
    best_params = extract_best_params(best_model.stages[-1].extractParamMap())
    print(f'Best params:\n{best_params}')

    results = cv_models.transform(test)
    print(f'CV Results on holdout dataset: {evaluation.evaluate(results)} (RMSE)')

    print('Re-fitting pipeline on entire dataset')
    cv_models = cv.fit(df)

    print('Saving to pipeline into S3')
    entire_dataset_best_model = cv_models.bestModel
    entire_dataset_best_model.save(f'{save}/{name}.v1')
    print('Done!')

    return


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--name', type=str, help='App or project name')
    parser.add_argument('--data', type=str, help='csv file location (e.g. s3://bucket/path/to/data)')
    parser.add_argument('--save', type=str, help='save location for spark pipeline object (e.g. s3://bucket/path/to)')
    args = parser.parse_args()

    run_pipeline(**args.__dict__)
