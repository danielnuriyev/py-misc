import json

import boto3

iam = boto3.client('iam')

n = 8
role_name = f"test-daniel-{n}"

role_exists = True
try:
    response = iam.get_role(RoleName=role_name)
except BaseException:
    role_exists = False

sts = boto3.client('sts')

if not role_exists:

    caller = sts.get_caller_identity()

    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Action": "sts:AssumeRole",
                "Principal": {
                    "Service": "s3.amazonaws.com"
                },
                "Effect": "Allow",
                "Sid": "0"
            },
            {
                "Action": "sts:AssumeRole",
                "Principal": {
                    "AWS":  f"{caller['Arn']}"
                },
                "Effect": "Allow",
                "Sid": "1"
            }
        ]
    }

    iam.create_role(
        Path='/',
        RoleName=role_name,
        AssumeRolePolicyDocument=json.dumps(policy),
    )

    policies = iam.list_user_policies(UserName='data.platform.kafka.confluent.prod')

    for name in policies["PolicyNames"]:
        policy = iam.get_user_policy(UserName='data.platform.kafka.confluent.prod', PolicyName=name)
        response = iam.put_role_policy(
            RoleName=role_name,
            PolicyName=name,
            PolicyDocument=json.dumps(policy["PolicyDocument"])
        )

import time
time.sleep(n)

response = iam.get_role(RoleName=role_name)
role_arn = response["Role"]["Arn"]

assumed_role_object = sts.assume_role(
    RoleArn=role_arn,
    RoleSessionName="any"
)
creds = assumed_role_object['Credentials']
print(creds)

s3_resource = boto3.resource(
    's3',
    aws_access_key_id=creds['AccessKeyId'],
    aws_secret_access_key=creds['SecretAccessKey'],
    aws_session_token=creds['SessionToken'],
)
s3_resource.Object("ss-bi-datalake-kafka", "test").put(Body=b'')
