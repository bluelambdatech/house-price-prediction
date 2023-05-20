"""Written by Omolewa"""

from ExtractLoad.utils.utils import ReadWriteFromS3, get_data
# from general_utils import load_yaml
# import botocore
import logging
# import yaml
# import pandas as pd
from datetime import datetime

logging.basicConfig(level=logging.INFO)


########    Data Extraction   ##################


def get_and_write_to_s3(bucket_name, key, url, filename):
    """
    This function extract the data from the url and write to s3
    :return: None
    """
    logging.info("Creating an S3 bucket")
    try:
        createbucket = ReadWriteFromS3.create_con_string(bucket_name, key)
        #TODO: use boto3 to list all the buckets
        createbucket.create_s3_bucket()
        logging.info("Created S3 successfully")
    except Exception as e:
        print("Bucket name already exist!!!")
        print(e)

    logging.info("Extracting the data from the URL")

    #url = load_yaml("param.yaml")["url"]
    df = get_data(url)

    logging.info("Done with data extraction")

    logging.info("Writing the dataframe to s3 bucket")
    writetos3 = ReadWriteFromS3.create_con_string(bucket_name, key)

    current_time = datetime.now().strftime("%d-%m-%Y:%H:%M:%S")
    filename = f'{filename}_{current_time}'
    writetos3.writeToS3(df=df,
                        file_name=filename)

    logging.info("Writing To S3 Bucket")

    # logging.info("Reading from S3 bucket")
    #
    # logging.info(f"Reading file with name {key} from S3 bucket")
    # readfroms3 = ReadWriteFromS3.create_con_string(bucket_name=bucket_name,
    #                                                key=key)
    # readfile = readfroms3.read_s3_file()
    # print(readfile)


if __name__ == "__main__":
    Bucket = "housepriceproject"
    Key = "dev/train2"
    get_and_write_to_s3(bucket_name=Bucket, key=Key)

# Tasks:
# 1. Write a method that will create an S3 bucket if it doesn’t exist already - DONE
# 2.  Write a code to extract the date column into day, month and year -DONE
# 3. Update the class to populate the other methods- read from s3.
