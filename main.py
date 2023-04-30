"""Written by Omolewa"""

from ExtractLoad.utils.utils import ReadWriteFromS3, get_data


########    Data Extraction   ##################

def get_and_write_to_s3():
    df = get_data("https://raw.githubusercontent.com/Amberlynnyandow/dsc-1-final-project-online-ds-ft-021119/master/kc_house_data.csv")

    writeToS3 = ReadWriteFromS3.create_con_string(bucket_name="houseprice23",
                                                  key="dev/train")

    writeToS3.writeToS3(df=df,
                        file_name="house_price")


if __name__ == "__main__":
    get_and_write_to_s3()

