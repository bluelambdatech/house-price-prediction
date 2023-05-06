"""Written by Omolewa"""
import io
from io import StringIO
import yaml
import boto3
import pandas as pd
from decouple import config
import logging
logging.basicConfig(level=logging.INFO)

logger = logging.info(__name__)

class ReadWriteFromS3:
    """This class is used to read and write to s3"""
    @classmethod
    def create_con_string(cls, bucket_name, key):
        secret_key = config("aws_secret_key")
        access_key = config("aws_access_key")

        s3_conn = boto3.resource("s3",
                                 aws_access_key_id=access_key, aws_secret_access_key=secret_key)

        #s3_conn =  s3.Object(bucket_name, key)
        logging.info("Creating the connection strings")

        return cls(conn=s3_conn, bucket_name=bucket_name, key=key)

    def __init__(self, conn, bucket_name, key):
        self.conn = conn
        self.bucket_name = bucket_name
        self.key = key


    def read_s3_file(self, num_row=None):
        """
         Reads a file from an S3 bucket and returns its contents as a string.
         These are the libraries required to use this function:
        """

        obj = self.conn.Object(self.bucket_name, self.key)
        buffer = io.BytesIO()
        file_ext = self.key.split(".")[-1]
        if file_ext in ["csv", "txt"]:
            df = pd.read_csv(io.BytesIO(obj.get()['Body'].read()))
        elif file_ext in ["xls", "xlsx"]:
            if file_ext == "xls":
                obj.download_fileobj(buffer)
                df = pd.read_excel(buffer)
            else:
                print(f"This file type {file_ext} cannot be handled at this time. Please try again later")
                exit(403)
        elif file_ext == "json":
            df = pd.read_json(io.BytesIO(obj.get()['Body'].read()))
        elif file_ext == "parquet":
            buffer = io.BytesIO(obj.get()['Body'].read())
            df = pd.read_parquet(buffer)
        elif file_ext in ["yaml", "yml"]:
            df = yaml.safe_load(obj.get()['Body'])
        else:
            print(f"This file type {file_ext} cannot be handled at this time. Please try again later")
            return None
        return df

    def cleanData(self):
        df = get_data("https://raw.githubusercontent.com/Amberlynnyandow/dsc-1-final-project-online-ds-ft-021119/master/kc_house_data.csv")
        df['date'] = pd.to_datetime(df['date'])
        df.insert(2, 'day', df['date'].dt.day)
        df.insert(3, 'month', df['date'].dt.month)
        df.insert(4, 'year', df['date'].dt.year)
        print(df.head())

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










