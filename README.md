# hw2_2581
#ข้อ1 สำหรับการสร้าง Bucket

import boto3
import botocore

s3 = boto3.client('s3')
s3_resource = boto3.resource('s3')

def create_bucket(bucket):
    import logging

    try:
        s3.create_bucket(Bucket=bucket)
    except botocore.expection.ClientError as e:
        logging.error(e)
        return 'Bucket ' + bucket + ' cloud not be created.'
    return 'Crated or already exists ' + bucket + ' bucket.'
create_bucket('nyctlc-cs653-2581')


#ข้อ 2 สำหรับการดึงข้อมูลไปเก็บไว้ที่ S3

def copy_among_buckets(from_bucket, from_key, to_bucket, to_key):
    s3_resource.meta.client.copy({'Bucket': from_bucket, 'Key': from_key},
                                        to_bucket, to_key)
    print(f'File {to_key} saved to S3 bucket {to_bucket}')

copy_among_buckets(from_bucket='nyc-tlc', from_key='trip data/yellow_tripdata_2017-01.parquet',
                      to_bucket='nyctlc-cs653-2581', to_key='trip data/yellow_tripdata_2017-01.parquet')

copy_among_buckets(from_bucket='nyc-tlc', from_key='trip data/yellow_tripdata_2017-02.parquet',
                      to_bucket='nyctlc-cs653-2581', to_key='trip data/yellow_tripdata_2017-02.parquet')

copy_among_buckets(from_bucket='nyc-tlc', from_key='trip data/yellow_tripdata_2017-03.parquet',
                      to_bucket='nyctlc-cs653-2581', to_key='trip data/yellow_tripdata_2017-03.parquet')


                                                                                                                            6,0-1         Top
