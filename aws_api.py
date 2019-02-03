from aws_requests_auth.aws_auth import AWSRequestsAuth

import json
import requests

auth = AWSRequestsAuth(aws_access_key="AKIAIHC4P2NEFQUO4WPA",
                       aws_secret_access_key="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",
                       aws_host="68298blcol.execute-api.us-east-1.amazonaws.com",
                       aws_region='us-east-1',
                       aws_service='execute-api')

response = requests.post('https://68298blcol.execute-api.us-east-1.amazonaws.com/dev/tokens',
                         auth=auth,
                         data=json.dumps({"subject": 10187601}))

response_obj = json.loads(response.content)
bearer = 'Bearer ' + response_obj["token"]

print(bearer)

response = requests.get(
    'https://dev-api.insightsquared.com/reports/v0.1/closing_opps/?_=1540306389649&date_tab=d7f',
    headers={'Authorization': bearer},
    verify=False
)

print(response.content)
