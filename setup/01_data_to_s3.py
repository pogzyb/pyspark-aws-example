# setup/01_data_to_s3.py
import boto3
import requests
import zipfile
import io
import os


#####################
# This script will:
# - download the .zip data files from capitalbikeshare's S3 bucket
# - unzip each file
# - put the unzipped csv's into your S3 bucket
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


def main() -> None:

    s3 = boto3.client('s3')

    bike_share_data_url = 'https://s3.amazonaws.com/capitalbikeshare-data/{file_name}'
    zip_file_base = '{year}-capitalbikeshare-tripdata.zip'

    for year in range(2010, 2018):
        file_name = zip_file_base.format(year=year)
        zip_file_url = bike_share_data_url.format(file_name=file_name)

        resp = requests.get(zip_file_url)
        zipf = zipfile.ZipFile(io.BytesIO(resp.content))

        archive_names = zipf.namelist()
        for name in archive_names:
            s3.put_object(
                Bucket=os.getenv('S3_BUCKET'),
                Key='bike-share-data/{name}'.format(name=name),
                Body=zipf.open(name),
                ContentType='text/csv'
            )

    return


if __name__ == '__main__':
    main()
