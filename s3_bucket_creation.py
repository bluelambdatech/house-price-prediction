import boto3
import os
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

def create_s3_bucket(bucket_name):
    """
    Creates an S3 bucket with the given name
    """
    # Create an S3 client
    s3 = boto3.resource('s3',
                        aws_access_key_id=os.getenv('access_key_id'),
                        aws_secret_access_key=os.getenv('secret_access_key'))

    # Create the bucket
    s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={'LocationConstraint': 'eu-west-1'})

    print(f'S3 bucket {bucket_name} created successfully')

bucket_name = "Nene_Bucket_00234"
#create_s3_bucket(bucket_name)