import base64
import json

import boto3
import mysql.connector
import requests

from datetime import timedelta

import prefect
from prefect import task, Flow, unmapped, context
from prefect.schedules import IntervalSchedule

@task
def get_secret(name, region):
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region
    )
    get_secret_value_response = client.get_secret_value(
        SecretId=name
    )
    if 'SecretString' in get_secret_value_response:
        secret = get_secret_value_response['SecretString']
    else:
        secret = base64.b64decode(get_secret_value_response['SecretBinary'])
    return json.loads(secret)


@task()
def fetch(secret, table, key, limit):

    offset = prefect.context.extra['offset']

    # secret = get_secret('BISandboxDatabase', 'us-east-1')

    con = mysql.connector.connect(
        host=secret['host'],
        user=secret['username'],
        passwd=secret['password'],
        database=secret['dbname']
    )
    cur = con.cursor(dictionary=True)
    cur.execute(f'SELECT * FROM {table} WHERE {key} > {offset} ORDER BY {key} LIMIT {limit}')
    rows = cur.fetchall()
    cur.close()
    con.close()

    prefect.context.extra['offset'] = rows[-1][key]

    return rows

@task
def transform(rows):

    rows = [rows]

    objs = []
    for row in rows:
        obj = {}
        for k, v in row.items():
            if type(v) == bytearray:
                v = v.decode()
            obj[k] = v
        objs.append(obj)
    return objs

@task
def post(secret, service, table, rows):

    rows = [rows]

    # secret = get_secret('oauth-client', 'us-east-1')

    response = requests.post('https://qa.auth.simplisafe.com/oauth/token', json=secret)
    response = json.loads(response.text)
    token = response['access_token']

    for row in rows:

        # https://github.com/spyoungtech/grequests
        response = requests.post(
            f'https://apis.prd.bi.simplisafe.com/data/v0/{service}/{table}',
            json=row,
            headers={
                'Authorization': f'Bearer {token}'
            }
        )


@task
def test(previous):
    print(prefect.context.extra['offset'])

schedule = IntervalSchedule(interval=timedelta(seconds=5))


with Flow("Test", schedule=schedule) as flow:
    db_secret = get_secret('BISandboxDatabase', 'us-east-1')
    fetch_task = fetch(db_secret, 'ss_location_AUDIT', 'vid', 1)
    # trasform_task = transform(fetch_task)
    trasform_task = transform.map(fetch_task)
    # post_task = post('test', 'test', trasform_task)
    api_secret = get_secret('oauth-client', 'us-east-1')
    post_task = post.map(secret=unmapped(api_secret), service=unmapped('test'), table=unmapped('test'), rows=trasform_task)
    test_task = test(post_task)

flow.visualize()

"""
flow.run(
    executor=DaskExecutor(
        address='tcp://10.0.0.77:8786'
    )
)
"""

# flow.register(project_name='test')

extra = {'offset': 0}
with prefect.context(extra=extra):
    state = flow.run()
# print(state)
# print(state.result[t])
# print(state.result[t].result)
