from pyspark.sql import *
from pyspark.sql import functions as f
from pyspark.sql.functions import *
import datetime
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import *
import json
import boto3
import datetime
from datetime import *
from reusable_functions import get_spark_session, get_config, get_secret, get_filename ,date_validation

print("Libraries imported..")

def extract_data(spark,file_path,delim=",",header=False):
    print("Inside extract_data function ...")
    input_df = spark.read.csv(file_path,sep = delim,header=header)
    return input_df

def move_file_to_preprocessed_folder(spark,config,input_file_path,preprocessed_file_path):
    print("Inside move_file_to_preprocessed_folder function")
    extract_data(spark,input_file_path,delim=config["delimiter"],header=True). \
    repartition(10). \
    write. \
    mode("overwrite"). \
    csv(preprocessed_file_path,sep="|",header=True)
    return None
    
def load_data(df,file_path):
    print("Inside load_data function")
    df. \
    repartition(10). \
    write. \
    mode("overwrite"). \
    csv(file_path,sep="|",header=True)
    return None
        
def transform_data(df,spark):
    print("Inside transform data function..") 
    validated_df = date_validation(df,"issue date","MM/dd/yyyy")
    final_df = validated_df.where("badRecord==false")
    return final_df

if __name__ == '__main__':
    spark = get_spark_session()
    config = get_config('nyc-taxi-data-analysis', 'config/config.json')
    print("configs collected..")
    secret_string = get_secret(str(config["secret_name"]),str(config["region_name"]))
    print("secret_string : ",secret_string)
    
    aws_access_key_id = json.loads(secret_string).get("aws_access_key_id")
    aws_secret_access_key = json.loads(secret_string).get("aws_secret_access_key")
    
    get_latest_file = get_filename(aws_access_key_id,aws_secret_access_key,config["bucket_name"],config["file_pattern"])
    
    print("processing file - ",get_latest_file)
    
    move_file_to_preprocessed_folder(spark,config,get_latest_file,config["preprocessed_file_path"])
    
    input_df = extract_data(spark,config["preprocessed_file_path"],delim="|",header=True)
      
    transformed_df = transform_data(input_df,spark)
    
    load_data(transformed_df,config["target_file_path"])
    
    #transform_data(input_df)
    print("File processing completed...")
