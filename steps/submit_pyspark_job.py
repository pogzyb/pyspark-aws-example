# steps/submit_pyspark_job.py
import boto3
import os

#####################
# This script will:
# - submit a "Step" or command to the EMR cluster (run the "bikeshare_ml_pipe.py" script)
#
# Note:
# These environment variables are assumed to be available
# in the shell or terminal where this script is ran:
#
# - S3_BUCKET
# - AWS_REGION
# - AWS_SECRET_KEY_ID
# - AWS_SECRET_ACCESS_KEY
#####################


def main() -> None:

    emr = boto3.client('emr')
    resp = emr.list_clusters(ClusterStates=['WAITING'])
    cluster_id = resp.get('Clusters')[0]['Id']

    job_name = 'bikeshare_ml'

    command = 'spark-submit /home/hadoop/bikeshare/emr-files/bikeshare_ml_pipe.py ' \
              f'--name  {job_name} ' \
              f'--data  s3://{os.getenv("S3_BUCKET")}/pyspark-example-problem/data ' \
              f'--save  s3://{os.getenv("S3_BUCKET")}/pyspark-example-problem/models'

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
        print('Successfully Submitted Job!')
    else:
        print(f'Uh oh!:\n{job_step_response}')

    return


if __name__ == '__main__':
    main()
