import sys
from io import BytesIO
from io import StringIO
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, FloatType, DecimalType
from pyspark.sql.functions import *
import datetime
import logging
import boto3
import pandas as pd
import numpy as np
import pymysql

args = getResolvedOptions(sys.argv, [
    'TempDir',
    'JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
logger = glueContext.get_logger()
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
current_date = (datetime.date.today()-datetime.timedelta(1)).strftime("%Y-%d-%m")

bucket_name = "dw-etl-cw-dev"
s3_file_name_main = "Process/AR/interface_ar_m1_bkp.dat"

s3_client = boto3.client('s3')
s3 = boto3.resource('s3')
bucket = s3.Bucket(bucket_name)
    
response = s3_client.get_object(Bucket=bucket_name, Key=s3_file_name_main)
file_content = response['Body'].read()

# Convert bytes to a Pandas DataFrame with ANSI encoding
df = pd.read_csv(BytesIO(file_content), encoding='latin1', delimiter='|')
filtered_df = df[df.iloc[:, 1] == '8346A93D-DBA1-4B45-BAD5-11B002DFCFB3']

# Print the DataFrame
print(filtered_df)
df_utf8_str = filtered_df.to_csv(encoding='utf-8', index=False)

# Read the string back into a new DataFrame with UTF-8 encoding
df_utf8 = pd.read_csv(StringIO(df_utf8_str), encoding='utf-8')

# Print the DataFrame with UTF-8 encoding
print(df_utf8)
spark_df = spark.createDataFrame(df_utf8)

# Show the PySpark DataFrame
spark_df.show()

# def processConcurReport(subfolder_name):
# 	data_xlsx = pd.DataFrame()
# 	for object_summary in bucket.objects.filter(Prefix=f"{subfolder_name}"):
# 		key = object_summary.key
# 		if key[-3:] == 'dat':
# 			full_key = 's3://'+bucket_name+'/'+key
# 			read_df = pd.read_csv(full_key, header = None, index_col = None)
# 			print(read_df)
			
# processConcurReport(s3_file_name_main)

