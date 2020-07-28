import boto3

r = boto3.resource("dynamodb", endpoint_url='http://172.17.0.5:8000/')

"""
r.create_table(
    TableName='ForecastConfiguration',
    KeySchema=[
        {
            'AttributeName': 'client',
            'KeyType': 'HASH'  #Partition key
        },
        {
            'AttributeName': 'view',
            'KeyType': 'RANGE'  #Sort key
        }
    ],
    AttributeDefinitions=[
        {
            'AttributeName': 'client',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'view',
            'AttributeType': 'S'
        },

    ],
    ProvisionedThroughput={
        'ReadCapacityUnits': 10,
        'WriteCapacityUnits': 10
    }
)
"""

t = r.Table("ForecastConfiguration")
print(t.item_count)