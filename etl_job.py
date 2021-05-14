

import sys
from pyspark.context import SparkContext
from pyspark.sql import *
from pyspark.sql import functions as f
from pyspark.sql.functions import *
import boto3
import pandas
import datetime
from datetime import *
import json
from ec2_metadata import ec2_metadata
from dq import *


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
        print("SecretString - ",SecretString)
        return SecretString
    except:
        print("Oops!", sys.exc_info()[0], "occurred.")


def get_parameters(ssm_client):
    """Load parameter values from AWS Systems Manager (SSM) Parameter Store"""
    print("Inside get parameters function ..")
    params = {
        'bucket_name': ssm_client.get_parameter(Name='/stockpricesanalysis/bucket')['Parameter']['Value'],
        'temp_file_path': ssm_client.get_parameter(Name='/stockpricesanalysis/temp_file_path')['Parameter']['Value'],
        'output_path': ssm_client.get_parameter(Name='/stockpricesanalysis/output_path')['Parameter']['Value'],
        'secret_name': ssm_client.get_parameter(Name='/stockpricesanalysis/secret_name')['Parameter']['Value'],
        'region_name': ssm_client.get_parameter(Name='/stockpricesanalysis/region_name')['Parameter']['Value'],
        'file_pattern': ssm_client.get_parameter(Name='/stockpricesanalysis/file_pattern')['Parameter']['Value']
    }
    return params

def load_data(final_df,output_path):
    print("Inside load_data function ...")
    final_df \
     .coalesce(1) \
     .write \
     .csv(output_path, mode='overwrite', header=True)
    return None

def transform_data(input_df):
    print("Inside transform_data function ...")
    input_df = date_validation(input_df)
    col_list = ["Open", "High", "Low", "Close", "AdjClose", "Volume"]
    input_df = NumberFormatting(input_df,col_list,spark)
    return input_df

def extract_data(spark,temp_file_path):
    print("Inside extract_data function ...")
    df = spark. \
        read.\
        csv(temp_file_path, header=True). \
        select("Date", "Open", "High", "Low", "Close", "AdjClose", "Volume")
    return df

def move_file_to_preprocess_folder(temp_file_path,filename,spark):
    print("Inside move_file_to_preprocess_folder function ...")
    tempdf = spark.read.csv(filename, header=True, sep=",")
    #temp_file_path = "s3://stockpricesanalysis/preprocessed/"
    tempdf.write.mode("overwrite").csv(temp_file_path, header=True)

# entry point for PySpark ETL application
if __name__ == '__main__':
    print("Inside main ..")
    print("Initializing SparkSession ...")
    spark = SparkSession.builder.appName("Nifty analytics").getOrCreate()
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
    #spark_context = SparkContext(pyFiles=['s3n://project-repository-stockpricesanalysis/scripts/stock-prices-analysis/jobs/dq.py'])
    #from dq import *
    # execute ETL pipeline
    print("ETL pipeline execution started")
    #os.environ['AWS_DEFAULT_REGION'] = ec2_metadata.region
    ssm_client = boto3.client('ssm',region_name = 'ap-south-1')
    config = get_parameters(ssm_client)
    #Get access details
    secret_string = get_secret(str(config["secret_name"]),str(config["region_name"]))
    aws_access_key_id = json.loads(secret_string).get("aws_access_key_id")
    aws_secret_access_key = json.loads(secret_string).get("aws_secret_access_key")
    print("aws_access_key_id : ",aws_access_key_id)
    print("aws_secret_access_key : ",aws_secret_access_key)
    #Identify the file to be processed
    bucket_name = config['bucket_name']  # "stockpricesanalysis"
    file_pattern = config['file_pattern']
    filename = get_filename(aws_access_key_id,aws_secret_access_key,bucket_name,file_pattern)
    #Move the file to preprocess folder
    print("processing file - ",filename)
    temp_file_path = config['temp_file_path']  #"s3://stockpricesanalysis/preprocessed/"
    move_file_to_preprocess_folder(temp_file_path,filename,spark)
    #Extract - Read the file from preprocess folder
    input_df = extract_data(spark,temp_file_path)
    #Transform - Data quality checks like column header checks, date format checks, number format checks
    data_transformed = transform_data(input_df)
    final_df = data_transformed \
    .where("badRecord == False") \
    .select("Date","Open","High","Low","Close","AdjClose","Volume")
    #Load - load to the target file path
    output_path = config['output_path']  #"s3://stockpricesanalysis/outbound/"
    load_data(final_df,output_path)
    #spark.stop()
    print("Data Processing completed ...")
#main()
