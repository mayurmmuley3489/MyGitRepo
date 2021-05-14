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
    last_modified_date = datetime(1900, 1, 1).replace(tzinfo=None) #set to a default date
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
    
def get_config(bucket_name,config_file_key):
    s3 = boto3.resource('s3')
    content_object = s3.Object(bucket_name,config_file_key)
    file_content = content_object.get()['Body'].read().decode('utf-8')
    config = json.loads(file_content)
    return config
    
def NumberFormatting(df,colList,spark):
    print("Inside function NumberFormatting..")
    for col in colList:
        df = df.withColumn(col, regexp_replace(col, '%', ''))
    return df

def date_validation(input_df,column_name,date_format):
    print("Inside date_validation function ...")
    input_df = input_df.withColumn("badRecord", f.when(f.to_date(f.col(column_name),date_format).isNotNull(), False).otherwise(True))
    return input_df

