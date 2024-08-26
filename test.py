import json
import logging
from datetime import datetime

import boto3

# from flask import Flask, request, jsonify

# app = Flask(__name__)

# Initialize logging
# logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel("INFO")

# List of models to try in case of failure
MODELS = [
    {"key":"meta","id":"meta.llama3-70b-instruct-v1:0","in_price":0.00099,"out_price":0.00099,"in_length":8*1024, "out_length":2048}
    ,{"key":"mistral","id":"mistral.mistral-large-2402-v1:0","in_price":0.004,"out_price":0.012, "in_length":32768, "out_length":8192}
    ,{"key":"amazon","id":"amazon.titan-text-premier-v1:0","in_price":0.0005,"out_price":0.0015,"in_length":32768, "out_length":3000}
    ,{"key":"cohere","id":"cohere.command-r-plus-v1:0","in_price":0.003,"out_price":0.015,"in_length":128000, "out_length":4096}
    ,{"key":"anthropic","id":"anthropic.claude-3-5-sonnet-20240620-v1:0","in_price":0.003,"out_price":0.015,"in_length":200000, "out_length":4096}
    ,{"key":"ai21","id":"ai21.jamba-instruct-v1:0","in_price":0.0005,"out_price":0.0007,"in_length":256000, "out_length":4096}
]

MODELS = list(reversed(MODELS))
models_dict = {model["key"]: model for model in MODELS}

# Initialize the Bedrock client
bedrock_client = boto3.client("bedrock-runtime")

# @app.route('/question', methods=['POST'])
# def handle_question():
#    return lambda_handler({"body": json.dumps(request.get_json())}, None)

def lambda_handler(event, context):

    start = datetime.now()

    print(f"Received event: {event}")

    event_body = json.loads(event['body'])
    request_text = event_body["text"]
    request_model = event_body.get("model", "ai21")

    request_id = context.aws_request_id

    # Log request details
    logger.info(f"Request {request_id} at {start}: {event_body}")

    model_candidates = [models_dict[request_model]] + [model for model in MODELS if model["key"] != request_model]

    for model in model_candidates:
        try:
            conversation = [
                {
                    "role": "user",
                    "content": [
                            {"text": json.dumps(request_text)},
                        ]
                }
            ]
                    
            response = bedrock_client.converse(
                modelId=model["id"],
                messages=conversation,
                inferenceConfig={
                    "maxTokens": model["out_length"],
                    },
            )
            usage = response["usage"]
            in_tokens = usage["inputTokens"]
            out_tokens = usage["outputTokens"]
            cost = in_tokens / 1000.0 * model["in_price"] + out_tokens / 1000.0 * model["out_price"]
            response_text = response["output"]["message"]["content"][0]["text"].strip()

            
            # Log successful response
            end = datetime.now()
            logger.info(f"Response {request_id} at {end} with {model['id']} taking {(end-start).total_seconds()} seconds costing ${cost:f}")

            # return jsonify({"text": response_text, "cost": cost}), 201
            return {
                "statusCode": 201,
                "body": json.dumps({"text": response_text, "model": model["id"], "cost": cost, "version": "0.0.0"}),
                'headers': {
                    'Content-Type': 'application/json',
                }
            }
        except Exception as e:
            # Log failure and try the next model
            logger.error(f"Model {model} failed at {datetime.now()}: {e}")

    # If all models fail, raise an HTTPException
    raise Exception("All models failed to process the request")

# if __name__ == "__main__":
#    app.run(debug=True)
