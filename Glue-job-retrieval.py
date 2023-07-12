import sys
import boto3
import csv
import io
import json


s3Path = "s3://dw-test-etl-job/Glue-job-retrieval"
glue_client = boto3.client('glue')
response = glue_client.list_jobs()
GlueList = []

job_list = response['JobNames']
while 'NextToken' in response:
    response = glue_client.list_jobs(NextToken=response['NextToken'])
    job_list.extend(response['JobNames'])
    
for job in job_list:
    response = glue_client.get_job(JobName=job)
    data = response["Job"]
    GlueList.append(data)

# Define headers for CSV
csv_headers = [
    "AllocatedCapacity", "Command", "Connections", "CreatedOn", "DefaultArguments",
    "Description", "ExecutionProperty", "GlueVersion", "LastModifiedOn",
    "MaxCapacity", "MaxRetries", "Name", "NumberOfWorkers", "Role", "Timeout", "WorkerType"
]

# Convert Glue job details to CSV
csv_data = []
csv_data.append(csv_headers)  # Add headers as the first row

for job in GlueList:
    csv_row = [
        job.get("AllocatedCapacity", ""),
        job.get("Command", {}).get("Name", ""),
        job.get("Connections", {}).get("Connections", []),
        job.get("CreatedOn", ""),
        json.dumps(job.get("DefaultArguments", {})),
        job.get("Description", ""),
        json.dumps(job.get("ExecutionProperty", {})),
        job.get("GlueVersion", ""),
        job.get("LastModifiedOn", ""),
        job.get("MaxCapacity", ""),
        job.get("MaxRetries", ""),
        job.get("Name", ""),
        job.get("NumberOfWorkers", ""),
        job.get("Role", ""),
        job.get("Timeout", ""),
        job.get("WorkerType", "")
    ]
    csv_data.append(csv_row)

# Prepare the file path and name
bucket_name = 'dw-test-etl-job'
file_name = 'Glue-job-retrieval/output.csv'

# Convert CSV data to a string
csv_buffer = io.StringIO()
csv_writer = csv.writer(csv_buffer)
csv_writer.writerows(csv_data)

# Upload the CSV file to S3
s3_client = boto3.client('s3')
s3_client.put_object(Body=csv_buffer.getvalue(), Bucket=bucket_name, Key=file_name)
