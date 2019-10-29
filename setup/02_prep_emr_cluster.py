# setup/02_prep_emr_cluster.py
import tarfile
import boto3
import os


###############################################################
# This script will:
# - tar and send the emr-files directory to s3 (exclude tar'ing the bootstrap.sh file)
# - send the bootstrap.sh file to s3 (not-tar'd)
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


def send_to_s3(client: boto3.client, local_file: str, s3_path_key: str, content_type: str) -> None:
    client.put_object(
        Bucket=os.getenv('S3_BUCKET'),
        Key=s3_path_key,
        Body=open(f'{local_file}', 'rb'),
        ContentType=content_type
    )
    return


def main() -> None:
    s3 = boto3.client('s3')

    bucket_project_folder = 'pyspark-example-problem'

    tar_filename = 'emr-files.tar.gz'
    bootstrap_emr_filename = 'bootstrap_emr.sh'

    with tarfile.open(tar_filename, 'w:gz') as tar_file_obj:
        for file in os.listdir('./emr-files'):
            if not file.startswith('bootstrap'):
                tar_file_obj.add(name=os.path.join('./emr-files', file))

    send_to_s3(
        client=s3,
        local_file=tar_filename,
        s3_path_key=f'{bucket_project_folder}/emr-files/{tar_filename}',
        content_type='application/x-tar'
    )

    send_to_s3(
        client=s3,
        local_file=f'emr-files/{bootstrap_emr_filename}',
        s3_path_key=f'{bucket_project_folder}/emr-files/{bootstrap_emr_filename}',
        content_type='text/x-shellscript'
    )
    print('Success - "emr-files" sent to S3!')
    return


if __name__ == '__main__':
    main()
