import boto3
import os
import pandas as pd
import time

workgroup = "..."
query_results_bucket = "..."

athena = boto3.client('athena')
s3 = boto3.client('s3')


def _query(sql):
    response = athena.start_query_execution(
        QueryString=sql,
        WorkGroup=workgroup,
        ResultConfiguration={
            'OutputLocation': f"s3://{query_results_bucket}/"
        }
    )
    id = response['QueryExecutionId']
    return id


def _wait(id):
    poll = True
    retry_interval = 1
    while poll:
        response = athena.get_query_execution(
            QueryExecutionId=id
        )
        state = response['QueryExecution']['Status']['State']
        if state == 'SUCCEEDED':
            return response['QueryExecution']['ResultConfiguration']['OutputLocation']
        elif state == 'FAILED':
            raise response
        else:
            time.sleep(retry_interval)
            retry_interval += 1


def _download(file):
    file = file[len('s3://'):]
    bucket, file = file.split('/')
    s3.download_file(bucket, file, file)
    return file


def _load(file):
    return pd.read_csv(file)


def _remove(file):
    os.remove(file)


def query(sql):
    query_id = _query(sql)
    file = _wait(query_id)
    file = _download(file)
    df = _load(file)
    _remove(file)
    return df

sql = "SELECT * FROM ..."
df = query(sql)
print(df)
