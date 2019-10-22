# steps/submit_pyspark_job.py
import boto3
import os

#####################
# This script will:
#
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


def main() -> None:

    emr = get_client('emr')

    job_name = 'bikeshare-ml'

    resp = emr.list_clusters(ClusterStates=['WAITING'])
    cluster_id = resp.get('Clusters')[0]['Id']

    command = 'spark-submit /home/hadoop/bikeshare_ml/emr_files/bikeshare_ml_pipe.py ' \
              f'--name  {job_name}' \
              f'--data  {os.getenv("S3_BUCKET")}/bike-share-data' \
              f'--save  {os.getenv("S3_BUCKET")}/models'

    job_step_response = emr.add_job_flow_steps(
        JobFlowId=cluster_id,
        Steps=[
            {
                'Name': job_name,
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                        'bash', '-c', command
                    ]
                }
            }
        ]
    )
    if job_step_response['ResponseMetadata']['HTTPStatusCode'] == 200:
        print('Successfully submitted Job!')
    else:
        print(f'Error!: {job_step_response}')

    return


if __name__ == '__main__':
    main()
