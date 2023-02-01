import boto3
import multiprocessing
import time

from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

db = "datalake_agg"


def count(table):
    try:
        athena = boto3.client('athena')
        sql = f'SELECT COUNT(*) FROM {db}."{table}"'
        start = time.time()
        id = athena.start_query_execution(
            QueryString=sql,
            ResultConfiguration={
                'OutputLocation': 's3://ss-bi-datalake-query-results/daniel.nuriyev/notebook/'
            },
        )['QueryExecutionId']

        running = True
        running_status = ['QUEUED', 'RUNNING']
        sleep = 1
        while running:
            # print(f'sleep {sleep}')
            time.sleep(sleep)
            sleep += 1
            status = athena.get_query_execution(QueryExecutionId=id)['QueryExecution']['Status']
            state = status['State']
            running = state in running_status

        if state == 'FAILED':
            print(status)
            print(f'finished {table} in {int(time.time() - start)} with {state}')
            return table, -1
        else:
            r = athena.get_query_results(QueryExecutionId=id)
            c = int(r['ResultSet']['Rows'][1]['Data'][0]['VarCharValue'])
            print(f'finished {table}:\t{c}\t{int(time.time() - start)} seconds')
            return table, c
    except BaseException as e:
        print(e)
        print(f'finished {table} in {int(time.time() - start)} with an Error')
        return table, -2


if __name__ == "__main__":
    glue = boto3.client('glue')
    # optimized_db = "datalake_optimized"
    optimized_db = "datalake_agg"
    response = glue.get_tables(DatabaseName=optimized_db)
    tables = []
    while True:
        tableList = response['TableList']
        for i in range(len(tableList)):
            name = tableList[i]['Name']
            tables.append(name)
            print(f"{len(tables)} {name}")
        next = response["NextToken"] if "NextToken" in response else None
        if not next: break
        response = glue.get_tables(DatabaseName=optimized_db, NextToken=next)

    cores =  multiprocessing.cpu_count()
    print(f"cores: {cores}")
    # pool = ThreadPoolExecutor(max_workers=cores * 2)
    pool = ProcessPoolExecutor(max_workers=cores-1)

    futures = []
    for table in tables:
        f = pool.submit(count, table)
        futures.append(f)

    start = time.time()
    counts = []
    for f in futures:
        r = f.result()
        counts.append(r)
        print(f"FINISHED {len(counts)} out of {len(tables)}")

    print(f"sorting {len(counts)}")
    counts.sort(key=lambda v: v[1], reverse=True)
    for i in range(len(counts)):
        r = counts[i]
        t = r[0]
        c = r[1]
        if c < 10000000: break
        print(f'{i}\t{t}\t{c}')
    print(f"finished {len(tables)} in {int(time.time() - start)} seconds")
