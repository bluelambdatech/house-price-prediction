"""Written by Omolewa"""

import boto3
import pandas as pd
from decouple import config

from io import StringIO

import logging
logging.basicConfig(level=logging.INFO)

logger = logging.info(__name__)

class ReadWriteFromS3:
    """This class is used to read and write to s3"""
    @classmethod
    def create_con_string(cls, bucket_name, key):
        secret_key = config("aws_secret_key")
        access_key = config("aws_access_key")

        s3_conn = boto3.resource("s3", aws_access_key_id = access_key, aws_secret_access_key = secret_key)
        logging.info("Creating the connection strings")

        return cls(conn=s3_conn, bucket_name=bucket_name, key=key)

    def __init__(self, conn, bucket_name, key):
        self.conn = conn
        self.bucket_name = bucket_name
        self.key = key

    def readFromS3(self):
        pass

    # def cleanData(self):
    #     df = get_data("https://raw.githubusercontent.com/Amberlynnyandow/dsc-1-final-project-online-ds-ft-021119/master/kc_house_data.csv")
    #     df['date'] = pd.to_datetime(df['date'])
    #     df.insert(2, 'day', df['date'].dt.day)
    #     df.insert(3, 'month', df['date'].dt.month)
    #     df.insert(4, 'year', df['date'].dt.year)
    #     print(df.head())

    def writeToS3(self, df, file_name):
        """
        This method writes any file type to s3 bucket
        :param df: pd.DataFrame -> dataframe to write to s3
        :param file_name: str -> file name for the dataframe to write to s3
        :return: None
        """
        file_name = f"{self.key}/{file_name}.csv"
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        logging.info("Writing the dataframe to s3 bucket")
        self.conn.Object(self.bucket_name, file_name).put(Body=csv_buffer.getvalue())

    def create_s3_bucket(self):
        """
        Creates an S3 bucket with the given name
        """
        self.conn.create_bucket(Bucket=self.bucket_name,
                                CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'})
        print(f'S3 bucket {self.bucket_name} created successfully')

def get_data(url):
    data_df = pd.read_csv(url, na_values=["nan", "n.a", "not available", "?", "NAN"])
    return data_df






