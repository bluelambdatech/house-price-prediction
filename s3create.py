import boto3
import os
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())


bucket_name = "uk-naija-testing-testing"

def create_s3_bucket(bucket_name):
    """
    Creates an S3 bucket with the given name
    """
    # Create an S3 client
    s3 = boto3.resource('s3',
                        aws_access_key_id=os.getenv('ACCESS_KEY_ID'),
                        aws_secret_access_key=os.getenv("SECRET_ACCESS_KEY"))
    #s3 = boto3.resource('s3')

    # Create the bucket
    s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'})

    print(f'S3 bucket {bucket_name} created successfully')

s3testing = create_s3_bucket("testing-testring-uknaija")
#print(s3testing)
