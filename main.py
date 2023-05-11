"""Written by Omolewa"""

from ExtractLoad.utils.utils import ReadWriteFromS3, get_data

import logging
logging.basicConfig(level=logging.INFO)

########    Data Extraction   ##################


def get_and_write_to_s3():
    """
    This function extract the data from the url and write to s3
    :return: None
    """
    logging.info("Extracting the data from the URL")
    df = get_data("https://raw.githubusercontent.com/Amberlynnyandow/dsc-1-final-project-online-ds-ft-021119/master/kc_house_data.csv")
    logging.info("Done with data extraction")

    logging.info("Writing the dataframe to s3 bucket")
    writetos3 = ReadWriteFromS3.create_con_string(bucket_name="housepriceproject",
                                                  key="dev/train2")

    writetos3.writeToS3(df=df,
                        file_name="house_price")

    logging.info("Done")


if __name__ == "__main__":
    get_and_write_to_s3()

