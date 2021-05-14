from pyspark.sql import *
from pyspark.sql import functions as f
from pyspark.sql.functions import *
import datetime
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import *

print("Libraries imported..")

def get_spark_session(app_name=""):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    return spark

def extract_data(spark,file_path,schema,delim=",",header=False):
    input_df = spark.read.csv(file_path,sep = delim,header=header,schema=schema)
    return input_df
    
spark = get_spark_session()

input_file_path = "s3://gap-indicator-analysis/outbound/part-00000-1b297ce5-1220-4cbf-8e9b-78db1f565b10-c000.csv"

schema = StructType([StructField("Date", StringType(), False),\
          StructField("Open", StringType(), False),\
          StructField("High", StringType(), False),\
          StructField("Low", StringType(), False),\
          StructField("Close", StringType(), False),\
          StructField("AdjClose", StringType(), False),\
          StructField("Volume", StringType(), False)
])

input_df = extract_data(spark,input_file_path,schema,",",True)
    
spark.sql("create database stock_analysis")

input_df.createOrReplaceTempView("stock_analysis.niftybees")

target_file_path = "s3://gap-indicator-analysis/outbound/"

spark.sql("
select * from stock_analysis.niftybees where gapindicator = 'Gap down'
"
).write.format("parquet").mode("overwrite").save(target_file_path)