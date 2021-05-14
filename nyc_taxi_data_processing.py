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

print("Libraries imported..")

def get_secret(secret_name,region_name):
    print("Inside get_secret function ...")
    print("secret_name :",secret_name)
    print("region_name :",region_name)
    try:
        # Create a Secrets Manager client
        print("Inside try...")
        session = boto3.session.Session(region_name=region_name)
        print("Session initialised ..")
        client = session.client(
            service_name='secretsmanager',
            region_name=region_name
        )
        print("CLIENT initialised ..")
        response = client.get_secret_value(SecretId=secret_name)
        print("response collected ..")
        SecretString = response.get("SecretString")
        return SecretString
    except:
        print("Oops!", sys.exc_info()[0], "occurred.")
        
def get_filename(aws_access_key_id,aws_secret_access_key,bucket_name,file_pattern):
    print("Inside get_filename function ...")
    print("Looking for file pattern - ",file_pattern)
    #Determine latest modified file
    s3 = boto3.resource('s3', aws_access_key_id=aws_access_key_id,
                        aws_secret_access_key=aws_secret_access_key)
    my_bucket = s3.Bucket(bucket_name)
    last_modified_date = datetime(1939, 9, 1).replace(tzinfo=None) #set to a default date
    for file in my_bucket.objects.all():
        if file_pattern.split("*")[0] in file.key and file_pattern.split("*")[1] in file.key:
            file_date = file.last_modified.replace(tzinfo=None)
            if last_modified_date < file_date:
                last_modified_date = file_date
    file_to_be_processed = ""
    for file in my_bucket.objects.all():
        # if config["file_pattern"] in file.key and ".csv" in file.key:
        if file_pattern.split("*")[0] in file.key and file_pattern.split("*")[1] in file.key:
            if file.last_modified.replace(tzinfo=None) == last_modified_date:
                file_to_be_processed = "s3://" + bucket_name + "/" + file.key
                #print("file_to_be_processed - ", file_to_be_processed)
    return  file_to_be_processed        

def get_spark_session(app_name=""):
    print("Inside get_spark_session function ...")
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    return spark

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
        
    
    
def get_config(cofig_file_path):
    s3 = boto3.resource('s3')
    content_object = s3.Object('nyc-taxi-data-analysis', 'config/config.json')
    file_content = content_object.get()['Body'].read().decode('utf-8')
    config = json.loads(file_content)
    return config
        

if __name__ == '__main__':
    spark = get_spark_session()
    config = get_config("s3://nyc-taxi-data-analysis/config/config.json")
    print("configs collected..")
    secret_string = get_secret(str(config["secret_name"]),str(config["region_name"]))
    print("secret_string : ",secret_string)
    
    aws_access_key_id = json.loads(secret_string).get("aws_access_key_id")
    aws_secret_access_key = json.loads(secret_string).get("aws_secret_access_key")
    
    get_latest_file = get_filename(aws_access_key_id,aws_secret_access_key,config["bucket_name"],config["file_pattern"])
    
    print("processing file - ",get_latest_file)
    
    move_file_to_preprocessed_folder(spark,config,get_latest_file,config["preprocessed_file_path"])
    
    input_df = extract_data(spark,config["preprocessed_file_path"],delim="|",header=True)
      
    
    load_data(input_df,config["target_file_path"])
    
    transform_data(input_df)
    print("File processing completed...")
