
import json
import urllib.parse

import boto3
import requests
from aws_requests_auth.aws_auth import AWSRequestsAuth

session = boto3.Session()
client = session.client('lambda')

def call_lambda(text, model="ai21"):
    
    data = {
        "body": json.dumps({"text": text, "model": model})
    }

    response = client.invoke(
        FunctionName="arn:aws:lambda:us-east-1:875029979081:function:bedrock-api-2",
        InvocationType='RequestResponse',
        Payload=json.dumps(data)
    )

    return json.loads(response['Payload'].read().decode('utf-8'))["body"]

def call_lambda_url(text, model="ai21"):

    url = "https://ywtck2t5fkngxb2c57y4n3qyim0egwik.lambda-url.us-east-1.on.aws/"

    credentials = boto3.Session().get_credentials()

    auth = AWSRequestsAuth(
        aws_access_key=credentials.access_key,
        aws_secret_access_key=credentials.secret_key,
        aws_token=credentials.token,
        aws_host=urllib.parse.urlparse(url).netloc,
        aws_region=session.region_name,
        aws_service="lambda"
    )

    data = {"text": text, "model": model}

    response = requests.post(
        url=url,
        auth=auth,
        data=json.dumps(data),
        headers={"Content-Type": "application/json"}
    )

    return response.text

def call_api_gw(text, model="ai21"):

    url = "https://ydgbhbihqh.execute-api.us-east-1.amazonaws.com/question"

    data = {
        "body": json.dumps({"text": text, "model": model})
    }

    response = requests.post(
        url=url,
        data=json.dumps(data),
        headers={"Content-Type": "application/json"}
    )

    return response.text


def main():

    history = []

    while True:
        
        q = input("Enter the question:").strip()
        if q in ("q", "quit"):
            break
        
        m = input("Enter the model (enter for ai21):").strip()
        if m not in ["ai21", "anthropic", "cohere", "amazon", "mistral", "meta"]:
            m = "ai21"

        context = ""
        if len(history) > 0:
            context = "Use this as the context:/n/n" + "/n/n".join(history)

        context += "/n/nUsing the context above, answer this question:/n/n" + q

        history.append(q)

        response = call_api_gw(q, m)
        print(response)

        history.append(response)
    

if __name__ == '__main__':
    main()

