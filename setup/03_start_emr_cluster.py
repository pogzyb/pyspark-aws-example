# setup/03_start_emr_cluster.py
import boto3
import json
import os
from typing import Dict, Union


###############################################################
# This script will:
# - start an EMR cluster using the boto3 EMR client with
# configurations as specified in "emr-config.json"
#
# Note:
# These environment variables are assumed to be available
# in the shell or terminal where this script is ran:
#
# - S3_BUCKET
# - AWS_REGION
# - AWS_SECRET_KEY_ID
# - AWS_SECRET_ACCESS_KEY
###############################################################


def format_s3_bucket(bootstrap_actions: list) -> list:
    for action in bootstrap_actions:
        action['ScriptBootstrapAction']['Path'] =\
            action['ScriptBootstrapAction']['Path'].format(s3_bucket=os.getenv('S3_BUCKET'))
        action['ScriptBootstrapAction']['Args'][0] = \
            action['ScriptBootstrapAction']['Args'][0].format(s3_bucket=os.getenv('S3_BUCKET'))
    return bootstrap_actions


def create_spark_cluster(configs: Dict[str, Union[str, int]]) -> None:

    copy_bs_actions = configs.get('bootstrap-actions')
    configs['bootstrap-actions'] = format_s3_bucket(copy_bs_actions)

    emr = boto3.client('emr')

    response = emr.run_job_flow(
        Name=configs.get('name'),
        ReleaseLabel='emr-5.27.0',
        Instances=configs.get('instances'),
        Applications=configs.get('applications'),
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='EMR_DefaultRole',
        VisibleToAllUsers=True,
        BootstrapActions=configs.get('bootstrap-actions')
    )
    if response['ResponseMetadata']['HTTPStatusCode'] == 200:
        print('Success - Creating Cluster!')
    else:
        print(f'Uh oh!:\n{response}')
    return


def main() -> None:
    configs = json.loads(open('../emr-config.json').read())
    create_spark_cluster(configs=configs)
    return


if __name__ == '__main__':
    main()
