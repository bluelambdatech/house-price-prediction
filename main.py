"""Written by Omolewa"""

from ExtractLoad.utils.utils import ReadWriteFromS3, get_data

import logging
import pandas as pd
logging.basicConfig(level=logging.INFO)

########    Data Extraction   ##################

bucket_name="uknaija-bucket-"
key="dev-uk-key"

def get_and_write_to_s3(bucket_name, key):
    pass
    """
    This function extract the data from the url and write to s3
    :return: None
    """
    logging.info("Creating an S3 bucket")
    try:
        createbucket = ReadWriteFromS3.create_con_string()
        createbucket.create_s3_bucket()
    except:
        print("bucket already exists!!!")

    logging.info("Created S3 successfully")

    logging.info("Extracting the data from the URL")

    df = get_data("https://raw.githubusercontent.com/Amberlynnyandow/dsc-1-final-project-online-ds-ft-021119/master/kc_house_data.csv")
    df['date'] = pd.to_datetime(df['date'])
    df.insert(2, 'day', df['date'].dt.day)
    df.insert(3, 'month', df['date'].dt.month)
    df.insert(4, 'year', df['date'].dt.year)
    print(df.head())

    logging.info("Done with data extraction")

    logging.info("Writing the dataframe to s3 bucket")
    writetos3 = ReadWriteFromS3.create_con_string(bucket_name="testing-123-nene-bucket",
                                                  key="dev/train")

    writetos3.writeToS3(df=df,
                       file_name="house_price")

    logging.info("Writing To S3 Bucket")

    logging.info("Reading from S3 bucket")
    bucket_name = "uk-naija-datascience-21032023"
    key = "uk-gdp-countries.parquet"
    print(f"Reading file with name {key} from S3 bucket")
    readfroms3 = ReadWriteFromS3.create_con_string(bucket_name=bucket_name,
                                                   key=key)
    readfile = readfroms3.read_s3_file()
    print(readfile)





if __name__ == "__main__":
    get_and_write_to_s3()


#Tasks:
#1. Write a method that will create an S3 bucket if it doesn’t exist already - DONE
#2.  Write a code to extract the date column into day, month and year -DONE
#3. Update the class to populate the other methods- read from s3.

