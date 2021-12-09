import boto3
s3 = boto3.resource('s3')
s3.meta.client.upload_file("s3.py", "ss-bi-datalake-dev", "tmp/test")