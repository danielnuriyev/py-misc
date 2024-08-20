import boto3
import os
import pandas as pd
import time

athena = boto3.client('athena')
s3 = boto3.client('s3')

def wait_for_query(id):
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
            print(response)
            raise response
        else:
            time.sleep(retry_interval)
            retry_interval += 1


def select(sql):
    response = athena.start_query_execution(
        QueryString=sql,
        WorkGroup='datalake'
    )
    id = response['QueryExecutionId']
    return wait_for_query(id)


def download(file):
    file = file[len('s3://'):]
    bucket, file = file.split('/')
    s3.download_file(bucket, file, file)
    return file


def load(file):
    return pd.read_csv(file)


def cleanup(file):
    os.remove(file)


def sql_to_pandas(sql):
    file = select(sql)
    file = download(file)
    df = load(file)
    cleanup(file)
    return df


# sql = 'SELECT COUNT(*) FROM datalake_playground.attrition_calls'
# df = sql_to_pandas(sql)
# print(df)
