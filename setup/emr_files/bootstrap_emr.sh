#!/usr/bin/env bash

echo "---> Starting Bootstrap Setup! <---"

s3files=${1}

echo "---> Setting up Python3 Virtual Environment <---"
sudo yum -y install python36
mkdir -p /home/hadoop/venv
cd /home/hadoop/venv
virtualenv -p /usr/bin/python3.6 py-ml


echo "---> Copying Scripts from S3 <---"
aws s3 cp ${s3files}/files.tar.gz /home/hadoop/files.tar.gz
mkdir -p /home/hadoop/py-ml
tar zxvf /home/hadoop/files.tar.gz -C /home/hadoop/py-ml

echo "---> Installing Python Libraries <---"
source /home/hadoop/venv/py-ml/bin/activate
pip3 install -r /home/hadoop/py-ml/emr-files/requirements.txt

set -a
. $PYSPARK_PYTHON=/usr/bin/python3.6
set +a

echo "---> Completed Bootstrap Setup! <---"