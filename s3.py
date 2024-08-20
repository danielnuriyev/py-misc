import boto3
import datetime
import dateutil
import pandas as pd
import sys


# Create an S3 client
s3_client = boto3.client('s3')

def count_versions():
    # The name of the bucket
    bucket_name = '...'
    prefixes = ['data/parquet/', 'data/orc/']

    # List top-level folders (prefixes)
    paginator = s3_client.get_paginator('list_objects_v2')

    total_count = 0
    for prefix in prefixes:

        operation_parameters = {
            'Bucket': bucket_name,
            'Delimiter': '/',
            'Prefix': prefix
        }

        page_iterator = paginator.paginate(**operation_parameters)

        for page in page_iterator:
            if 'CommonPrefixes' in page:
                for page_prefix in page['CommonPrefixes']:
                    print(page_prefix['Prefix'])

                    operation_parameters = {
                        'Bucket': bucket_name,
                        'Delimiter': '/',
                        'Prefix': page_prefix['Prefix']
                    }

                    inner_pages = paginator.paginate(**operation_parameters)
                    for inner_page in inner_pages:
                        if 'CommonPrefixes' in inner_page:
                            version_count = 0
                            for inner_page_prefix in inner_page['CommonPrefixes']:
                                t = inner_page_prefix['Prefix'].split("/")[-2]
                                if len(t) > 4:
                                    
                                    version_count += 1
                                    
                                    operation_parameters = {
                                        'Bucket': bucket_name,
                                        'Delimiter': '/',
                                        'Prefix': inner_page_prefix['Prefix']
                                    }

                                    file_pages = paginator.paginate(**operation_parameters)
                                    for file_page in file_pages:
                                        if 'Contents' in file_page:
                                            count = len(file_page['Contents'])
                                            total_count += count

                            if version_count > 1:
                                print(f"versions: {version_count}")

    print(f"files: {total_count}")

def count_ext():
   
    bucket_name = '...'
    
    # List top-level folders (prefixes)
    paginator = s3_client.get_paginator('list_objects_v2')

    operation_parameters = {
            'Bucket': bucket_name,
            'Delimiter': '/',
            'Prefix': 'data/orc/braze_campaigns_uk/metadata/'
        }

    page_iterator = paginator.paginate(**operation_parameters)

    count = 0
    for page in page_iterator:
        for file in page['Contents']:
            if file['Key'].endswith('json'):
                count += 1

    print(count)

def count_reloaded():

    # The name of the bucket
    bucket_name = '...'
    prefixes = ['data/parquet/', 'data/orc/']

    # List top-level folders (prefixes)
    paginator = s3_client.get_paginator('list_objects_v2')

    total_count = 0
    for prefix in prefixes:

        operation_parameters = {
            'Bucket': bucket_name,
            'Delimiter': '/',
            'Prefix': prefix
        }

        page_iterator = paginator.paginate(**operation_parameters)

        for page in page_iterator:
            if 'CommonPrefixes' in page:
                for page_prefix in page['CommonPrefixes']:
                    print(page_prefix['Prefix']) # data/parquet/table/

                    operation_parameters = {
                        'Bucket': bucket_name,
                        'Delimiter': '/',
                        'Prefix': page_prefix['Prefix']
                    }

                    inner_pages = paginator.paginate(**operation_parameters)
                    for inner_page in inner_pages:
                        if 'CommonPrefixes' in inner_page:
                            for inner_page_prefix in inner_page['CommonPrefixes']:
                                t = inner_page_prefix['Prefix'].split("/")[-2]
                                if len(t) > 4 \
                                    and not t.startswith("da") \
                                    and not t.startswith("year") \
                                    and datetime.datetime.utcfromtimestamp(int(t)) > datetime.datetime.utcnow() - datetime.timedelta(days = 1):

                                    print(inner_page_prefix['Prefix'])

                                    operation_parameters = {
                                        'Bucket': bucket_name,
                                        'Delimiter': '/',
                                        'Prefix': inner_page_prefix['Prefix']
                                    }

                                    page_iterator = paginator.paginate(**operation_parameters)
                                    count = 0
                                    for page in page_iterator:
                                        count += len(page['Contents'])
                                    print(count)
                                    total_count += count
                                    print(total_count)


    print(f"files: {total_count}")

def load_inventory():

    buckets = ["", "-optimized", "-agg", "-pii", "-pii-optimized", "-pii-agg"]

    paginator = s3_client.get_paginator('list_objects_v2')

    bucket = "..."

    bucket_counts = {}

    for b in buckets:

        b = f"...{b}"

        path = f"{b}/data/data/"
        
        operation_parameters = {
            'Bucket': bucket,
            'Delimiter': '/',
            'Prefix': path
        }

        page_iterator = paginator.paginate(**operation_parameters)
        
        count = 0

        for page in page_iterator:

            for file in page['Contents']:

                if file["Key"].endswith("/") or file["LastModified"].replace(tzinfo=datetime.timezone.utc) < (datetime.datetime.now() - datetime.timedelta(days=1)).replace(tzinfo=datetime.timezone.utc):
                    continue

                p = f"s3://{bucket}/{file['Key']}"
                print(p)

                df = pd.read_parquet(p)

                all_file_count = len(df)
                
                df['last_modified_date'] = df['last_modified_date'].dt.floor('d')

                max_day = df['last_modified_date'].max()

                df = df[df["last_modified_date"] == max_day]

                print(f"{len(df)}/{all_file_count}")
                count += len(df)

        print(f"{b}: {count}")
        bucket_counts[b] = count

    total = 0
    for bucket,count in bucket_counts.items():
        print(f"{bucket}: {count}")
        total += count

    print(total)
   
load_inventory()