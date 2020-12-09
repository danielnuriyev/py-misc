import base64
import boto3
import json
from datetime import datetime
import time

my_stream_name = 'test'

kinesis_client = boto3.client('kinesis', region_name='us-east-1')

response = kinesis_client.describe_stream(StreamName=my_stream_name)

print(response['StreamDescription']['Shards'])

my_shard_id = response['StreamDescription']['Shards'][0]['ShardId']

shard_iterator = kinesis_client.get_shard_iterator(StreamName=my_stream_name,
                                                      ShardId=my_shard_id,
                                                      ShardIteratorType='AT_TIMESTAMP',
                                                   Timestamp=time.time() - 12 * 60 * 60)
# above the stream is new, there is no danger of reading old records

my_shard_iterator = shard_iterator['ShardIterator']

total = 0
while True:
    record_response = kinesis_client.get_records(ShardIterator=my_shard_iterator, Limit=1)

    my_shard_iterator = record_response['NextShardIterator']
    records = record_response["Records"]
    print("record count: " + str(len(records)))
    for r in records:
        data = r["Data"]
        print("data: " + str(data))
        # decoded = data.decode("UTF-8")
    total += len(records)
    print("total " + str(total))

    # wait a bit
    time.sleep(0.1)
