import logging
import os

from datetime import datetime
from logging.handlers import RotatingFileHandler
from typing import List, Dict

import boto3

from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

def _setup_logging():
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


class Bedrock():

    def __init__(self):

        # set up the models
        self.models = [
            {"key":"meta","id":"meta.llama3-70b-instruct-v1:0","in_price":0.00099,"out_price":0.00099,"in_length":8*1024, "out_length":2048}
            ,{"key":"mistral","id":"mistral.mistral-large-2402-v1:0","in_price":0.004,"out_price":0.012, "in_length":32768, "out_length":8192}
            ,{"key":"amazon","id":"amazon.titan-text-premier-v1:0","in_price":0.0005,"out_price":0.0015,"in_length":32768, "out_length":3000}
            ,{"key":"cohere","id":"cohere.command-r-plus-v1:0","in_price":0.003,"out_price":0.015,"in_length":128000, "out_length":4096}
            ,{"key":"anthropic","id":"anthropic.claude-3-5-sonnet-20240620-v1:0","in_price":0.003,"out_price":0.015,"in_length":200000, "out_length":4096}
            ,{"key":"ai21","id":"ai21.jamba-instruct-v1:0","in_price":0.0005,"out_price":0.0007,"in_length":256000, "out_length":4096}
        ]
        # find the maximum context length
        self.longest_model = sorted(self.models, key=lambda x: x["in_length"])[-1]
        # create a set of models for quick access
        self.model_names = {model["key"] for model in self.models}

        # set up the bedrock client
        self._client = boto3.client("bedrock-runtime", region_name=os.environ.get('AWS_REGION'))

    def call(self, models, context):

        context_text_length = sum([len(c["text"]) for c in context])

        for i in range(len(models)):

            current_model = models[i]

            if context_text_length > current_model["in_length"]:
                if i == len(models) - 1:
                    current_model = self.longest_model
                else:
                    continue

            conversation = [
                    {
                        "role": "user",
                        "content": [{"text": c["text"]} for c in context]
                    }
                ]
            
            try:
                
                response = self._client.converse(
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

class ContextManager():

    def __init__(self, bedrock):
        self._bedrock = bedrock
        self._contexts = {}
        self._user_models = {}
        self._model_dict = {model["key"]: model for model in self._bedrock.models}

    def get_context(self, context_id):
        return self._contexts.get(context_id, [])
    
    def set_context(self, context_id, context):
        context, _ = self.trim_context(context)
        self._contexts[context_id] = context

    def get_model(self, context_id):
        if context_id in self._user_models:
            return self._user_models[context_id]
        else:
            context = self._contexts.get(context_id, [])
            models = self.sort_models(context_id, context)
            return models[0]
    
    def set_model(self, context_id, model_key):
        self._user_models[context_id] = self._model_dict[model_key]

    # @classmethod
    def trim_context(self, context :List[Dict[str,str]]):
        context_text_length = 0
        for i in range(len(context)):
            idx = i + 1
            context_text_length += len(context[-idx]["text"])
            if context_text_length > self._bedrock.longest_model["in_length"]:
                context_text_length -= len(context[-idx]["text"])
                if i == 0:
                    context = []
                else:
                    context = context[-i:]
                break
        return context, context_text_length

    def sort_models(self, context_id, context=None):
        
        """
        Sorts the models by price for a user.
        If the user specified a model, put it first.
        """

        if context is None:
            context = self.get_context(context_id)

        if len(context) == 0:
            sorted_by_price = sorted(self._bedrock.models, key=lambda x: x["in_price"])
        else:
            in_length = 0
            out_length = 0
            for c in context:
                if c["type"] == "in":
                    in_length += len(c["text"])
                else:
                    out_length += len(c["text"])

            sorted_by_price = sorted(
                self._bedrock.models, key=lambda x: x["in_price"] * in_length + x["out_price"] * out_length)
        

        models = []
        if context_id in self._user_models:
            models.append(self._user_models[context_id])
            for model in sorted_by_price:
                if model["key"] != self._user_models[context_id]["key"]:
                    models.append(model)
        else:
            models.extend(sorted_by_price)
        
        return models
        
class Slack():

    def __init__(self, bedrock):
        self._context_manager = ContextManager(bedrock)
        self._bedrock = bedrock

    # @app.command("/testllm")
    def ask(self, args):

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
        context = self._context_manager.get_context(context_id)

        # create context for this question
        current_context = []
        current_context.extend(context)
        current_context.append({"text":question,"type":"in"})

        # make sure that the current context does not exceed the max length
        current_context, context_text_length = self._context_manager.trim_context(current_context)
        
        # get the cheapest model for this channel:user
        models = self._context_manager.sort_models(context_id, current_context)

        try:
        
            # send the question with the context to bedrock
            answer = self._bedrock.call(models, current_context)
        
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
            args.logger.info(
                f"Response at {end} for {context_id} with {answer['model']} taking {(end-start).total_seconds()} seconds costing ${answer['cost']:f}")

            # form the latest context
            context.append({"text":question,"type":"in"})
            context.append({"text":answer['text'],"type":"out"})

            # save the context per channel:user
            self._context_manager.set_context(context_id, context) 

        except Exception as e:
            args.say(f"Error: {e}")
        

    # @app.command("/llmc")
    def clear(self, args):
        args.ack()
        channel = args.body["channel_name"]
        user = args.body["user_name"]
        context_id = f"{channel}:{user}"
        self._context_manager.set_context(context_id, [])
        args.say("Clear the context")

    # @app.command("/llmm")
    def model(self, args):
        args.ack()
        channel = args.body["channel_name"]
        user = args.body["user_name"]
        context_id = f"{channel}:{user}"
        if not len(args.body["text"].strip()):
            models = self._context_manager.sort_models(context_id)
            model_name = self._context_manager.get_model(context_id)['key']
            args.say(f"Currently using {model_name}. You can use one of {', '.join([model['key'] for model in models])}")
        elif args.body["text"] in self._bedrock.model_names:
            self._bedrock.set_model(context_id, args.body["text"])
            args.say(f"Model set to {args.body['text']}")
        else:
            args.say(f"Use one of {', '.join(self._bedrock.model_names)}")

    # @app.command("/llmh")
    def help(self, args):
        args.ack()
        args.say("""
    Use /llm to ask a question.
    Use /llmc to clear the context.
    Use /llmm to get the current model or to change the model.The models are sorted by price.
        """)
    
if __name__ == "__main__":
    _setup_logging()
    app = App(token=os.environ.get("SLACK_BOT_TOKEN"))
    bedrock = Bedrock()
    slack = Slack(bedrock)
    app.command("/llm")(slack.ask)
    app.command("/llmc")(slack.clear)
    app.command("/llmm")(slack.model)
    app.command("/llmh")(slack.help)
    slack = SocketModeHandler(app, os.environ["SLACK_APP_TOKEN"])
    slack.start()