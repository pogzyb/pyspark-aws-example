# notebook/data/make_sample_set.py
import pandas as pd
import requests
import zipfile
import random
import io


def main():
    bike_share_data_url = 'https://s3.amazonaws.com/capitalbikeshare-data/{file_name}'
    zip_file_base = '{year}-capitalbikeshare-tripdata.zip'

    full_sample_set = pd.DataFrame()
    percent = 0.5
    random.seed(333)

    for year in range(2010, 2018):
        file_name = zip_file_base.format(year=year)
        zip_file_url = bike_share_data_url.format(file_name=file_name)

        resp = requests.get(zip_file_url)
        zipf = zipfile.ZipFile(io.BytesIO(resp.content))

        archive_names = zipf.namelist()
        for name in archive_names:
            tmp = pd.read_csv(
                zipf.open(name),
                header=0,
                skiprows=lambda i: i > 0 and random.random() < percent
            )
            full_sample_set = pd.concat([full_sample_set, tmp], sort=False, ignore_index=True)

    print(f'Full Sample Set (rows, columns): {full_sample_set.shape}')
    full_sample_set.to_csv('./rides/sample_set.csv', index=False)

    print('Success - saved sample set')
    return


if __name__ == '__main__':
    main()
