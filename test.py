import boto3
import pandas as pd

# Initialize the Athena client
athena_client = boto3.client('athena')

# Define the database and table
database = '...'
table = 'dagster_all_downstream_assets'

# Define the initial list of downstream assets
downstream_assets = [
    #
]

# Function to execute Athena query and return results as a DataFrame
def execute_athena_query(query):
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database},
        ResultConfiguration={'OutputLocation': 's3://.../'}
    )
    query_execution_id = response['QueryExecutionId']
    query_status = None
    while query_status not in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
        response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        query_status = response['QueryExecution']['Status']['State']
        if query_status in ['FAILED', 'CANCELLED']:
            raise Exception(f"Query failed or was cancelled: {response['QueryExecution']['Status']['StateChangeReason']}")
    results = athena_client.get_query_results(QueryExecutionId=query_execution_id)
    columns = [col['Name'] for col in results['ResultSet']['ResultSetMetadata']['ColumnInfo']]
    rows = [[field['VarCharValue'] for field in row['Data']] for row in results['ResultSet']['Rows'][1:]]
    return pd.DataFrame(rows, columns=columns)

# Function to recursively select all upstream assets
def get_all_upstream_assets(downstream_assets):
    all_upstream_assets = set(downstream_assets)
    while downstream_assets:

        print(f"Downstream assets: {len(downstream_assets)}")

        query = f"""
        SELECT upstream_asset
        FROM {database}.{table}
        WHERE downstream_asset IN ({','.join([f"'{asset}'" for asset in downstream_assets])})
        """
        results = execute_athena_query(query)
        upstream_assets = set(results['upstream_asset'].tolist())
        downstream_assets = upstream_assets - all_upstream_assets
        all_upstream_assets.update(downstream_assets)

        print(f"All upstream assets: {len(all_upstream_assets)}")
        
    return sorted(list(all_upstream_assets))

# Get all upstream assets
upstream_assets = get_all_upstream_assets(downstream_assets)
for asset in upstream_assets:
    print(asset)
print(len(upstream_assets))
