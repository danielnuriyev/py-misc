import logging
import os

from datetime import datetime
from logging.handlers import RotatingFileHandler
from typing import List

import boto3

from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

# set up logging
file_handler = RotatingFileHandler("slack_bedrock.log", maxBytes=10*1024*1024, backupCount=100)
file_handler.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
# formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# file_handler.setFormatter(formatter)
# console_handler.setFormatter(formatter)
root_logger = logging.getLogger()
root_logger.addHandler(file_handler)
root_logger.addHandler(console_handler)

# set up the models
MODELS = [
    {"key":"meta","id":"meta.llama3-70b-instruct-v1:0","in_price":0.00099,"out_price":0.00099,"in_length":8*1024, "out_length":2048}
    ,{"key":"mistral","id":"mistral.mistral-large-2402-v1:0","in_price":0.004,"out_price":0.012, "in_length":32768, "out_length":8192}
    ,{"key":"amazon","id":"amazon.titan-text-premier-v1:0","in_price":0.0005,"out_price":0.0015,"in_length":32768, "out_length":3000}
    ,{"key":"cohere","id":"cohere.command-r-plus-v1:0","in_price":0.003,"out_price":0.015,"in_length":128000, "out_length":4096}
    ,{"key":"anthropic","id":"anthropic.claude-3-5-sonnet-20240620-v1:0","in_price":0.003,"out_price":0.015,"in_length":200000, "out_length":4096}
    ,{"key":"ai21","id":"ai21.jamba-instruct-v1:0","in_price":0.0005,"out_price":0.0007,"in_length":256000, "out_length":4096}
]
# find the maximum context length
MODELS.sort(key=lambda x: x["in_length"])
max_length = MODELS[-1]["in_length"]
# sort the models by price
MODELS.sort(key=lambda x: x["in_price"])
# the cheapest model will be the default
default_model = MODELS[0]["key"]
# create a dictionary of models for quick access
models_dict = {model["key"]: model for model in MODELS}

# set up the bedrock client
bedrock_client = boto3.client("bedrock-runtime", region_name="us-east-1") # TODO: region from env

def _trim_context(context :List[str], max_length):
    context_text_length = 0
    for i in range(len(context)):
        idx = i + 1
        context_text_length += len(context[-idx])
        if context_text_length > max_length:
            context_text_length -= len(context[-idx])
            if i == 0:
                context = []
            else:
                context = context[-i:]
            break
    return context, context_text_length

def call_bedrock(model, context, context_text_length):

    model_candidates = [models_dict[model]] + [model for model in MODELS if model["key"] != model]

    for current_model in model_candidates:

        if context_text_length > current_model["in_length"]:
            continue

        conversation = [
                {
                    "role": "user",
                    "content": [{"text": text} for text in context]
                }
            ]
        
        try:
            
            response = bedrock_client.converse(
                modelId=current_model["id"],
                messages=conversation,
                inferenceConfig={
                    "maxTokens": current_model["out_length"],
                    },
            )
            usage = response["usage"]
            in_tokens = usage["inputTokens"]
            out_tokens = usage["outputTokens"]
            cost = in_tokens / 1000.0 * current_model["in_price"] + out_tokens / 1000.0 * current_model["out_price"]
            response_text = response["output"]["message"]["content"][0]["text"].strip()

            return {"text": response_text, "model": current_model["id"], "cost": cost}
        except Exception as e:
            # Log failure and try the next model
            print(f"Model {current_model} failed at {datetime.now()}: {e}")

    # If all models fail, raise an HTTPException
    raise Exception("All models failed to process the request")


contexts = {}
user_models = {}

app = App(token=os.environ.get("SLACK_BOT_TOKEN"))

@app.command("/ask")
def ask(args):

    start = datetime.now()
    
    args.ack()
    
    channel = args.body["channel_name"]
    user = args.body["user_name"]
    question = args.body["text"]
    
    if not len(question.strip()):
        args.say("Please provide a question.")
        return
    
    context_id = f"{channel}:{user}"

    args.logger.setLevel("INFO")
    args.logger.info(f"Request at {datetime.now()} from {context_id}: {question}")

    # get the context of this channel:user
    context = contexts.get(context_id, [])

    # create context for this question
    current_context = []
    current_context.extend(context)
    current_context.append(question)

    # make sure that the current context does not exceed the max length
    current_context, context_text_length = _trim_context(current_context, max_length)
    
    # get the model for this channel:user
    model = user_models.get(context_id, default_model)

    try:
    
        # send the question with the context to bedrock
        answer = call_bedrock(model, current_context, context_text_length)
    
        # form the response to Slack
        # answer["context_length"] = context_text_length
        blocks = {
            "text":answer['text'],
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Question:*\n{question}"
                        }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Response:*\n{answer['text']}"
                        }
                },
                {
                    "type": "section",
                    "fields": [
                        {
                            "type": "mrkdwn",
                            "text": f"*Asked by:* {user}"
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*Model:* {answer['model']}"
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*Cost:* ${answer['cost']:f}"
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*Context length:* {context_text_length}"
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*Duration:* {(datetime.now()-start).total_seconds()} seconds"
                        }
                    ]
                }
            ]
        }

        # respond to Slack
        args.say(blocks)

        # log
        end = datetime.now()
        args.logger.info(f"Response at {end} for {context_id} with {answer['model']} taking {(end-start).total_seconds()} seconds costing ${answer['cost']:f}")

        # form the latest context
        context.append(question)
        context.append(answer["text"])

        # save the context per channel:user
        contexts[context_id], _ = _trim_context(context, max_length)

    except Exception as e:
        args.say(f"Error: {e}")
    

@app.command("/reset")
def reset(args):
    args.ack()
    channel = args.body["channel_name"]
    user = args.body["user_name"]
    context_id = f"{channel}:{user}"
    contexts[context_id] = ""
    args.say("Context reset")

@app.command("/model")
def model(args):
    args.ack()
    channel = args.body["channel_name"]
    user = args.body["user_name"]
    cache_id = f"{channel}:{user}"
    if not len(args.body["text"].strip()):
        args.say(f"Currently using {user_models.get(cache_id, default_model)}. You can use one of {', '.join(models_dict.keys())}")
    elif args.body["text"] in models_dict.keys():
        user_models[cache_id] = args.body["text"]
        args.say(f"Model set to {args.body['text']}")
    else:
        args.say(f"Use one of {', '.join(models_dict.keys())}")

@app.command("/help")
def help(args):
    args.ack()
    args.say("""
Use /ask to ask a question.
Use /reset to clear the context.
Use /model to get the current model or to change the model.The models are sorted by price.
    """)
    
    
# Start your app
if __name__ == "__main__":
    SocketModeHandler(app, os.environ["SLACK_APP_TOKEN"]).start()
    # print(call_bedrock("amazon", ["hi"], 0))