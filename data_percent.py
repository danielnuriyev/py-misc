import boto3
import concurrent.futures
import time

class AthenaDataExplorer:
    def __init__(self, database, s3_output_bucket, region_name='us-east-1'):
        """
        Initialize Athena Explorer with database and S3 output configuration
        
        :param database: Name of the Athena database
        :param s3_output_bucket: S3 bucket for query results
        :param region_name: AWS region 
        """
        self.athena = boto3.client('athena', region_name=region_name)
        self.s3 = boto3.client('s3', region_name=region_name)
        self.database = database
        self.s3_output_bucket = s3_output_bucket
        self.s3_output_prefix = '.../'

    def _execute_query(self, query):
        """
        Execute an Athena query and return query execution ID
        
        :param query: SQL query to execute
        :return: Query execution ID
        """

        response = self.athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': self.database},
            ResultConfiguration={
                'OutputLocation': f's3://{self.s3_output_bucket}/{self.s3_output_prefix}'
            }
        )

        return response['QueryExecutionId']

    def _wait_for_query(self, query_execution_id):
        """
        Wait for Athena query to complete
        
        :param query_execution_id: ID of the query execution
        :return: Query result details
        """
        t = 1
        while True:
            response = self.athena.get_query_execution(QueryExecutionId=query_execution_id)
            status = response['QueryExecution']['Status']['State']
            
            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                if status == 'FAILED':
                    raise Exception(f"Query Failed: {response['QueryExecution']['Status']['StateChangeReason']}")
                
                scanned_bytes = response['QueryExecution']['Statistics']['DataScannedInBytes']
                return scanned_bytes

            time.sleep(t ** 2)  # Wait before checking again
            t += 1

    def list_tables_and_views(self):
        """
        List all tables and views in the database
        
        :return: Dictionary of tables and views
        """
        tables_query = f"SHOW TABLES IN {self.database}"
        views_query = f"SHOW VIEWS IN {self.database}"
        
        # TODO: use athena_boto3 instead
        tables_exec_id = self._execute_query(tables_query)
        views_exec_id = self._execute_query(views_query)
        
        # Wait for queries to complete
        self._wait_for_query(tables_exec_id)
        self._wait_for_query(views_exec_id)
        
        # Fetch results
        tables_result = self.athena.get_query_results(QueryExecutionId=tables_exec_id)
        views_result = self.athena.get_query_results(QueryExecutionId=views_exec_id)
        
        tables = [row['Data'][0]['VarCharValue'] for row in tables_result['ResultSet']['Rows'][1:]]
        views = [row['Data'][0]['VarCharValue'] for row in views_result['ResultSet']['Rows'][1:]]

        tables = [table for table in tables if not table.startswith("z_")]
        views = [view for view in views if not view.startswith("z_")]

        print(len(tables))
        print(len(views))

        return sorted(list(set(tables + views)))

    def query_and_copy_data(self, output_bucket):
        """
        Query 1 record from each table/view and copy results
        
        :param output_bucket: S3 bucket to copy query results
        :return: Total bytes scanned
        """
        tables_and_views = self.list_tables_and_views()
        pool = concurrent.futures.ThreadPoolExecutor(max_workers=8)
        futures = []
        for item in tables_and_views:
    
            def _query(table):

                try:

                    start = time.time()

                    # Query 1 record
                    query = f'SELECT * FROM {self.database}."{table}" TABLESAMPLE BERNOULLI(1)'
                    query_exec_id = self._execute_query(query)
                    
                    # Wait for query and track scanned bytes
                    scanned_bytes = self._wait_for_query(query_exec_id)
                    
                    # Copy result to output bucket
                    source_key = f"{self.s3_output_prefix}{query_exec_id}.csv"
                    dest_key = f"output/{table}_sample.csv"
                    
                    self.s3.copy_object(
                        Bucket=output_bucket,
                        CopySource={
                            'Bucket': self.s3_output_bucket, 
                            'Key': source_key
                        },
                        Key=dest_key
                    )

                    cur_mb = round((scanned_bytes * 1.0) / 1024.0 / 1024.0, 2)
                    print(f"{table} with {cur_mb}mb during {round(time.time() - start, 2)}s")

                    return table, scanned_bytes
                
                except Exception as e:
                    print(f"Error querying {table}")
                    return table, 0

            futures.append(pool.submit(_query, table=item))

            # if len(futures) >= 8 * 4: break

        counter = 0
        total_scanned_bytes = 0
        start = time.time()
        for future in futures:
            item, scanned_bytes = future.result()

            counter += 1
            total_scanned_bytes += scanned_bytes
                
            cur_mb = round((scanned_bytes * 1.0) / 1024.0 / 1024.0, 2)
            total_mb = round((total_scanned_bytes * 1.0) / 1024.0 / 1024.0, 2)
            cur_time = round((time.time() - start)/(counter*1.0), 2)
            total_time = round((time.time() - start), 2)

            print(f"{counter}/{len(tables_and_views)}: {item} with {cur_mb}mb/{total_mb}mb at {cur_time}s per query since {total_time}s")

        pool.shutdown(wait=True)

        return total_scanned_bytes

def main():
    # Configure your AWS settings
    database_name = '...'
    query_output_bucket = '...'
    output_results_bucket = '...'

    explorer = AthenaDataExplorer(database_name, query_output_bucket)

    explorer.query_and_copy_data(output_results_bucket)


if __name__ == "__main__":
    main()