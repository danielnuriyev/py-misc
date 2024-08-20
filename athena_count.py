import boto3
import multiprocessing
import time

from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

def count(table):
    print(f"starting {table}")
    try:
        athena = boto3.client('athena')
        sql = f'SELECT COUNT(*) FROM {table}'
        start = time.time()
        id = athena.start_query_execution(
            QueryString=sql,
            ResultConfiguration={
                'OutputLocation': 's3://.../'
            },
        )['QueryExecutionId']

        running = True
        running_status = ['QUEUED', 'RUNNING']
        sleep = 0
        while running:
            if sleep > 0: 
                print(f"sleeping {table} for {sleep}")
            time.sleep(sleep)
            status = athena.get_query_execution(QueryExecutionId=id)['QueryExecution']['Status']
            state = status['State']
            running = state in running_status
            sleep += 1

        if state == 'FAILED':
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
    db = "datalake_pii_agg"
    response = glue.get_tables(DatabaseName=db)
    tables = []
    while True:
        tableList = response['TableList']
        for i in range(len(tableList)):
            name = tableList[i]['Name']
            if name.startswith("z_"): continue
            # if not name.startswith("device_"): continue
            tables.append(f'{db}."{name}"')
            print(f"{len(tables)} {name}")
        next = response["NextToken"] if "NextToken" in response else None
        if not next: break
        response = glue.get_tables(DatabaseName=db, NextToken=next)

    cores =  multiprocessing.cpu_count()
    print(f"cores: {cores}")
    # pool = ThreadPoolExecutor(max_workers=cores * 2)
    pool = ProcessPoolExecutor(max_workers=8)

    futures = []
    for table in tables:
        print(f"ADDING {table}")
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
 #      if c < 100000000: break
        print(f'{i}\t{t}\t{c}')
    print(f"FINISHED ALL {len(tables)} in {int(time.time() - start)} seconds")
