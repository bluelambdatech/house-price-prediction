from ExtractLoad import extract_main
from FE.utils.utils import FeatureEngineering
from general_utils import load_yaml

##### Extract ##############

params = load_yaml("param.yaml")

bucket = params["s3_params"]["bucket_name"]
Key = params["s3_params"]["Key"]


extract_main.get_and_write_to_s3(bucket_name=bucket, key = Key)


#### Transform #######

# Load the dataframe that you wrote to s3
df =
transform = FeatureEngineering(df)
transform.run_process()
# w,rite back to s3


#### Model
