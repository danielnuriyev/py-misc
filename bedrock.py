import ast
import boto3
import datetime
from botocore.exceptions import ClientError

import athena

limit = 10

df = athena.load(athena.select(f"""
                SELECT query_string
                FROM "datalake_optimized"."de_table_query_usage"
                where _partitioned__date = '2024/08/18'
                and contains(tables, 'INVALID QUERY')
                order by length(query_string) desc
                limit {limit*10}""" # TODO: query in pages
            ))

client = boto3.client("bedrock-runtime", region_name="us-east-1")

# model = {"id":"meta.llama3-70b-instruct-v1:0","in":0.00099,"out":0.00099,"in_length":8*1024, "out_length":2048}
# model = {"id":"mistral.mistral-large-2402-v1:0","in":0.004,"out":0.012, "in_length":32768, "out_length":8192}
# model = {"id":"amazon.titan-text-premier-v1:0","in":0.0005,"out":0.0015,"in_length":32768, "out_length":3000}
# model = {"id":"cohere.command-r-plus-v1:0","in":0.003,"out":0.015,"in_length":128000, "out_length":4096}
# model = {"id":"anthropic.claude-3-5-sonnet-20240620-v1:0","in":0.003,"out":0.015,"in_length":200000, "out_length":4096}
model = {"id":"ai21.jamba-instruct-v1:0","in":0.0005,"out":0.0007,"in_length":256000, "out_length":4096}

tool_list = [
    {
        "toolSpec": {
            "name": "parse_sql",
            "description": "parses sql and returns the table names",
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "tables": {
                            "type": "string",
                            "description": "a python list of strings that contains a list of table names"
                        }
                    },
                    "required": [
                        "tables",
                    ]
                }
            }
        }
    }
]

start = datetime.datetime.now()
in_tokens = 0
out_tokens = 0
count = 1

for query in df["query_string"].tolist():

    if len(query) > model["in_length"]:
        print(f"Query length {len(query)} exceeds model input length {model['in_length']}")
        continue

    conversation = [
        {
            "role": "user",
            "content": [
                    {"text": f"<content>{query}</content>"},
                    {"text": """Extract the table names and return them as a python list of strings.  
    Return the python list only without any additional text in the response."""},
                ]
        }
    ]

    try:
        # Send the message to the model, using a basic inference configuration.
        response = client.converse(
            modelId=model["id"],
            messages=conversation,
            inferenceConfig={
                "maxTokens": model["out_length"],
                # "stopSequences":[],
                # "temperature":0.5,
                # "topP":0.25
                },
            #         toolConfig={
            #             "tools": tool_list,
            #             "toolChoice": {
            #                 "tool": {
            #                     "name": "parse_sql",
            #                 }
            #             }
            #         }
        )

        usage = response["usage"]
        in_tokens += usage["inputTokens"]
        out_tokens += usage["outputTokens"]
        
        response_text = response["output"]["message"]["content"][0]["text"].strip()
        if response_text.endswith("."):
            response_text = response_text[:-1]
        try:
            tables = set(ast.literal_eval(response_text))
        except Exception as e:
            print(response_text)
            print(f"Can't parse '{response_text}': {e}")
        else:
            print(f"{count}: {tables}")

    except (ClientError, Exception) as e:
        print(f"ERROR: Can't invoke '{model['id']}'. Reason: {e}")
        exit(1)

    if count % 10 == 0:
        print(f"Model: {model['id']}")
        print(f"Number of queries: {count}")
        print(f"Elapsed time: {datetime.datetime.now() - start}")
        print(f"Input tokens: {in_tokens}, cost: {in_tokens / 1000.0 * model['in']}")
        print(f"Output tokens: {out_tokens}, cost: {out_tokens / 1000.0 * model['out']}")
        print(f"Total cost: {in_tokens / 1000.0 * model['in'] + out_tokens / 1000.0 * model['out']}")
        print(f"Time per query: {(datetime.datetime.now() - start).total_seconds() / limit}")
        print(f"Cost per query: {(in_tokens / 1000.0 * model['in'] + out_tokens / 1000.0 * model['out']) / limit}")

    count += 1

    if count > limit:
        break