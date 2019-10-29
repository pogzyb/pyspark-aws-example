# setup/01_send_data_to_s3.py
import boto3
import requests
import zipfile
import io
import os


###############################################################
# This script will:
# - download the .zip data files from capitalbikeshare's S3 bucket
# - unzip each file (into an "archive" or folder of csv's)
# - put the extracted csv's into "s3://<your-bucket-name>/bike-share-data/*"
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


def main() -> None:

    s3 = boto3.client('s3')

    bucket_project_folder = 'pyspark-example-problem'

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
                Key=f'{bucket_project_folder}/data/rides/{name}',
                Body=zipf.open(name),
                ContentType='text/csv'
            )

    stations_csv_name = 'capbs_stations.csv'
    stations_csv_path = '../notebook/data/stations'

    s3.put_object(
        Bucket=os.getenv('S3_BUCKET'),
        Key=f'{bucket_project_folder}/data/stations/{stations_csv_name}',
        Body=open(os.path.join(os.getcwd(), stations_csv_path, stations_csv_name), 'rb'),
        ContentType='text/csv'
    )

    print('Success - copied data to S3!')
    return


if __name__ == '__main__':
    main()
