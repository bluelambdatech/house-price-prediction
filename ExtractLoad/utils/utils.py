"""Written by Omolewa"""

import boto3
import pandas as pd
from decouple import config

from io import StringIO


class ReadWriteFromS3:
    """This class is used to read and write to s3"""
    @classmethod
    def create_con_string(cls, bucket_name, key):
        secret_key = config("aws_secret_key")
        access_key = config("aws_access_key")

        s3_conn = boto3.resource("s3", aws_access_key_id = access_key, aws_secret_access_key = secret_key)

        return cls(conn=s3_conn, bucket_name=bucket_name, key=key)

    def __init__(self, conn, bucket_name, key):
        self.conn = conn
        self.bucket_name = bucket_name
        self.key = key

    def readFromS3(self):
        pass

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
        self.conn.Object(self.bucket_name, file_name).put(Body=csv_buffer.getvalue())

    def create_bucket(self, bucket_name):
        pass


def get_data(url):
    data_df = pd.read_csv(url, na_values=["nan", "n.a", "not available", "?", "NAN"])
    return data_df






