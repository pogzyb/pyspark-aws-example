# setup/03_start_emr_cluster.py
import boto3
import json
import os


#####################
# This script will:

# Note:
# These environment variables are assumed to be available
# in the shell or terminal where this script is ran:
#
# - S3_BUCKET
# - AWS_REGION
# -
# -
# -
#####################


def get_client(service: str) -> boto3.client:
    client = boto3.client(service)
    return client


def create_spark_cluster(**kwargs) -> None:
    emr = get_client('emr')
    response = emr.run_job_flow(
        Name=kwargs.get('cluster_name'),
        ReleaseLabel="emr-5.27.0",
        Instances={
            'InstanceGroups': [
                {
                    'Name': 'EmrMaster',
                    'Market': 'SPOT',
                    'InstanceRole': 'MASTER',
                    'BidPrice': '0.45',
                    'InstanceType': kwargs.get('master_node_type'),
                    'InstanceCount': 1,
                    'Configurations': [
                        {
                            "Classification": "spark-env",
                            "Configurations": [
                                {
                                    "Classification": "export",
                                    "Properties": {
                                        "PYSPARK_PYTHON": "/usr/bin/python3.6"
                                    }
                                }
                            ]
                        }
                    ]
                },
                {
                    'Name': 'EmrCore',
                    'Market': 'SPOT',
                    'InstanceRole': 'CORE',
                    'BidPrice': '0.45',
                    'InstanceType': kwargs.get('worker_node_type'),
                    'InstanceCount': kwargs.get('worker_node_count')
                }
            ],
            'Ec2KeyName': kwargs.get('keypair_name'),
            'KeepJobFlowAliveWhenNoSteps': True
        },
        Applications=[{'Name': 'Hadoop'}, {'Name': 'Spark'}],
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='EMR_DefaultRole',
        VisibleToAllUsers=True,
        BootstrapActions=[
            {
                'Name': 'setup',
                'ScriptBootstrapAction': {
                    'Path': f's3://{os.getenv("S3_BUCKET")}/emr_files/bootstrap_emr.sh',
                    'Args': [f's3://{os.getenv("S3_BUCKET")}/emr_files', 'emr_files.tar.gz']
                }
            }
        ]
    )
    if response['ResponseMetadata']['HTTPStatusCode'] == 200:
        print('Success - Creating Cluster!')
    else:
        print('Uh oh!')
    return


def main() -> None:
    config = json.loads('../emr-config.json')
    create_spark_cluster(**config)
    return


if __name__ == '__main__':
    main()
