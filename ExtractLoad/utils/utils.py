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

        s3_conn = boto3.client("s3", aws_access_key_id = access_key, aws_secret_access_key = secret_key)
        logging.info("Creating the connection strings")

        return cls(conn=s3_conn, bucket_name=bucket_name, key=key)

    def __init__(self, conn, bucket_name, key):
        self.conn = conn
        self.bucket_name = bucket_name
        self.key = key


    def read_s3_file(self, num_row=None):
        """
        Reads a file from an S3 bucket and returns its contents as a pandas dataframe.
        :param num_rows: int -> number of rows to return
        :return: pd.DataFrame
        """
        obj = self.conn.get_object(Bucket=self.bucket_name, Key=self.key)
        buffer = io.BytesIO()
        file_ext = self.key.split(".")[-1]
        if file_ext in ["csv", "txt"]:
            df = pd.read_csv(obj['Body'])
            if df.shape[0] == 0:
                exit(500)
        elif file_ext in ["xls", "xlsx"]:
            df = pd.read_excel(io.BytesIO(obj['Body'].read()))
        elif file_ext == "json":
            num_row = None
            df = pd.read_json(obj['Body']).to_dict()
        elif file_ext == "parquet":
            object = self.conn.Object(bucket_name=self.bucket_name, key=self.key)
            object.download_fileobj(buffer)
            df = pd.read_parquet(buffer)
        elif file_ext in ["yaml", "yml"]:
            if num_row:
                print(f"This file - {self.key} cannot be handled. Please try again without num_rows specified")
                exit(500)
            else:
                df = yaml.safe_load(obj["Body"])
        else:
            print(f"This file type {file_ext} cannot be handled at this time. Please try again later")
            exit(500)
        if num_row:
            return df.head(num_row)
        return df  ## End of function

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






