import base64
import boto3
import eks_token
import json
import kubernetes.client
import tempfile

cluster_name = "..."
role_arn = "arn:aws:iam::...:role/..."

eks = boto3.client('eks')
cluster_info = eks.describe_cluster(name=cluster_name)
cluster_endpoint = cluster_info['cluster']['endpoint']
cert_authority = cluster_info['cluster']['certificateAuthority']['data']
ca_cert = tempfile.NamedTemporaryFile(delete=True)
with open(ca_cert.name, 'wb') as f:
    f.write(base64.b64decode(cert_authority))

token = eks_token.get_token(
    cluster_name=cluster_name,
    role_arn=role_arn
)['status']["token"]

configuration = kubernetes.client.Configuration()
configuration.api_key['authorization'] = token
configuration.api_key_prefix['authorization'] = 'Bearer'
configuration.host = cluster_endpoint
configuration.ssl_ca_cert = ca_cert.name

with kubernetes.client.ApiClient(configuration) as api_client:
    api_instance = kubernetes.client.CoreV1Api(api_client)

    api_response = api_instance.list_namespace()
    for namespace_data in api_response.items:
        print(namespace_data.metadata.name)
