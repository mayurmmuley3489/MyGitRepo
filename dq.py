import sys
#from awsglue.transforms import *
#from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
#from awsglue.context import GlueContext
#from awsglue.job import Job
from pyspark.sql import *
from pyspark.sql import functions as f
from pyspark.sql.functions import *
import boto3
import pandas
import datetime
from datetime import *
import json


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

def NumberFormatting(df,colList,spark):
    print("Inside function NumberFormatting..")
    # Convert pyspark dataframe to pandas df
    df_pd = df.toPandas()
    # Remove $ from price columns
    for col in colList:
        df_pd[col] = df_pd[col].str.replace('$', '')
    # Convert pandas dataframe to pyspark df
    df_sp = spark.createDataFrame(df_pd)
    return df_sp

def date_validation(input_df):
    print("Inside date_validation function ...")
    dateFormattingUDF = udf(lambda z: dateFormatting(z))
    input_df = input_df.withColumn("Date",dateFormattingUDF(col("Date")))
    input_df = input_df.withColumn("badRecord", f.when(f.to_date(f.col("Date"),"dd-MM-yyyy").isNotNull(), False).otherwise(True))
    return input_df


def dateFormatting(input):
    print("Inside function dateFormatting ..")
    day = input.split("-")[0].zfill(2)
    month = input.split("-")[1].zfill(2)
    year = input.split("-")[2]
    finaldate = day+"-"+month+"-"+year
    return finaldate
