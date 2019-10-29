#!/usr/bin/env bash

echo "--->  Starting Bootstrap Setup!   <---"

S3_LOCATION=${1}
EMR_ZIP_FILES=${2}

echo "--->  Copying Scripts from S3!    <---"

mkdir -p /home/hadoop/bikeshare
aws s3 cp ${S3_LOCATION}/${EMR_ZIP_FILES} /home/hadoop/${EMR_ZIP_FILES}
tar zxvf /home/hadoop/${EMR_ZIP_FILES} -C /home/hadoop/bikeshare

echo "--->  Completed Bootstrap Setup!  <---"
