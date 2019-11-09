#!/usr/bin/env bash

echo "--->  Starting Bootstrap Setup!   <---"

S3_LOCATION=${1}
EMR_ZIP_FILES=${2}

echo "--->  Copying Scripts from S3!    <---"

mkdir -p /home/hadoop/bikeshare
aws s3 cp ${S3_LOCATION}/${EMR_ZIP_FILES} /home/hadoop/${EMR_ZIP_FILES}
tar zxvf /home/hadoop/${EMR_ZIP_FILES} -C /home/hadoop/bikeshare

echo "--->  Installing Python Libraries <---"

SPARK_HOME=/usr/local/spark
JPMML_VERSION=1.5.4
wget https://github.com/jpmml/jpmml-sparkml/releases/download/${JPMML_VERSION}/jpmml-sparkml-executable-${JPMML_VERSION}.jar \
    && mv jpmml-sparkml-executable-${JPMML_VERSION}.jar ${SPARK_HOME}/jars/jpmml-sparkml-executable-${JPMML_VERSION}.jar

easy_install-3.6 pip
pip install py4j==0.10.7 pyspark2pmml==0.5.1

echo "--->  Completed Bootstrap Setup!  <---"
