import boto3
import json
from datetime import datetime
import calendar
import random
import time

my_stream_name = 'datalake_ss_events'

kinesis_client = boto3.client('kinesis', region_name='us-east-1')

def put_to_stream(property_value):
    payload = {
                'prop': str(property_value),
              }

    kinesis_client.put_record(
                        StreamName=my_stream_name,
                        Data=json.dumps(payload),
                        PartitionKey="test")

i = 0
while i < 10000:

    print(i)

    put_to_stream(i)

    i += 1
