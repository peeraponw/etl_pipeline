import pandas as pd
import os
from io import StringIO, BytesIO
import boto3
from datetime import datetime
from prefect import task, flow


bucket_name='fullstackdata2023'
my_name='student1'

source_path = 'common/data/full/full_transaction.csv'


year = datetime.now().year
month = datetime.now().month
day = datetime.now().day
target_path = f'{my_name}/customer/snap={year}_{month}_{day}.parquet'

# Retrieve AWS credentials from environment variables
aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')


# Create a Boto3 session
session = boto3.Session(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)
# Create an S3 client
s3_client = session.client('s3')

# # # # # # # # # # # # # # # # # #
# # Read file from s3
# # # # # # # # # # # # # # # # # #
@task(retries=3, retry_delay=timedelta(seconds=10))
def extract(bucket_name, source_path):
    # Get the object from S3
    response = s3_client.get_object(Bucket=bucket_name, Key=source_path)
    # Read the CSV file content
    csv_string = StringIO(response['Body'].read().decode('utf-8'))
    # Use Pandas to read the CSV file
    df = pd.read_csv(csv_string)
    return df

# # # # # # # # # # # # # # # # # #
# # calculate the customer table
# # # # # # # # # # # # # # # # # #
customer_cols = ['customer_id', 'customer_name', 'customer_province']
@task
def transform(df, customer_cols):
    customer = df[customer_cols].drop_duplicates()
    return customer

# # # # # # # # # # # # # # # # # #
# # write to s3 as parquet format
# # # # # # # # # # # # # # # # # #
@task
def load(customer, bucket_name, target_path):
    # Convert the DataFrame to a Parquet file in memory
    parquet_buffer = BytesIO()
    customer.to_parquet(parquet_buffer, index=False)
    # Upload the Parquet file to S3
    s3_client.put_object(Bucket=bucket_name, Key=target_path, Body=parquet_buffer.getvalue())

# # # # # # # # # # # # # # # # # #
# # main pipeline
# # # # # # # # # # # # # # # # # #
@flow(log_prints=True)
def pipeline():
    df = extract(bucket_name, source_path)
    customer = transform(df, customer_cols)
    load(customer, bucket_name, target_path)
    
if __name__ == '__main__':
    pipeline()
    # pipeline.visualize()
    pipeline.serve()