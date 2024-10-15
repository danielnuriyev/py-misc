import argparse
import logging
import json
import os
import pickle

from datetime import datetime
from logging.handlers import RotatingFileHandler
from typing import List, Dict

import boto3
import requests

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


class Model():
    def __init__(self, key, model_id, in_price, out_price, in_length, out_length):
        self.key = key
        self.model_id = model_id
        self.in_price = in_price
        self.out_price = out_price
        self.in_length = in_length
        self.out_length = out_length

class Bedrock():

    def __init__(self):

        # set up the models
        self.models = [
            Model(key="meta", model_id="meta.llama3-70b-instruct-v1:0", in_price=0.00099, out_price=0.00099, in_length=8*1024, out_length=2048),
            Model(key="mistral", model_id="mistral.mistral-large-2402-v1:0", in_price=0.004, out_price=0.012, in_length=32768, out_length=8192),
            Model(key="amazon", model_id="amazon.titan-text-premier-v1:0", in_price=0.0005, out_price=0.0015, in_length=32768, out_length=3000),
            Model(key="cohere", model_id="cohere.command-r-plus-v1:0", in_price=0.003, out_price=0.015, in_length=128000, out_length=4096),
            Model(key="anthropic", model_id="anthropic.claude-3-5-sonnet-20240620-v1:0", in_price=0.003, out_price=0.015, in_length=200000, out_length=4096),
            Model(key="ai21", model_id="ai21.jamba-instruct-v1:0", in_price=0.0005, out_price=0.0007, in_length=256000, out_length=4096)
        ]
        # find the maximum context length
        self.longest_model = sorted(self.models, key=lambda x: x.in_length)[-1]
        # create a set of models for quick access
        self.model_names = {model.key for model in self.models}

        # set up the bedrock client
        self._client = boto3.client("bedrock-runtime", region_name=os.environ.get('AWS_REGION'))

    def call(self, models, context):

        context_text_length = sum([len(c.text) for c in context])

        for i in range(len(models)):

            current_model = models[i]

            if context_text_length > current_model.in_length:
                if i == len(models) - 1:
                    current_model = self.longest_model
                else:
                    continue

            conversation = [
                    {
                        "role": "user",
                        "content": [{"text": c.text} for c in context]
                    }
                ]
            
            try:
                
                response = self._client.converse(
                    modelId=current_model.model_id,
                    messages=conversation,
                    inferenceConfig={
                        "maxTokens": current_model.out_length,
                        },
                )
                usage = response["usage"]
                in_tokens = usage["inputTokens"]
                out_tokens = usage["outputTokens"]
                cost = in_tokens / 1000.0 * current_model.in_price + out_tokens / 1000.0 * current_model.out_price
                response_text = response["output"]["message"]["content"][0]["text"].strip()

                return {"text": response_text, "model": current_model.model_id, "cost": cost}
            except Exception as e:
                # Log failure and try the next model
                print(f"Model {current_model} failed at {datetime.now()}: {e}")

        # If all models fail, raise an HTTPException
        raise Exception("All models failed to process the request")

class ContextItem():

    def __init__(self, text, type):
        self.text = text
        self.type = type # TODO: use an enum

    def to_dict(self):
        return {"text": self.text, "type": self.type}

    @classmethod
    def from_dict(cls, data):
        return cls(text=data["text"], item_type=data["type"])


class ContextManager():

    def __init__(self, bedrock):
        self._bedrock = bedrock

        if os.path.exists("contexts.pkl"):
            with open('contexts.pkl', 'rb') as f:
                self._contexts = pickle.load(f)
        else:
            self._contexts = {}

        if os.path.exists("moels.pkl"):
            with open('models.pkl', 'rb') as f:
                self._user_models = pickle.load(f)
        else:   
            self._user_models = {}

        self._model_dict = {model.key: model for model in self._bedrock.models}

    def get_context(self, context_id):
        return self._contexts.get(context_id, [])
    
    def set_context(self, context_id, context):
        context, _ = self.trim_context(context)
        self._contexts[context_id] = context

        with open('contexts.pkl', 'wb') as f:
            pickle.dump(self._contexts, f)

    def get_model(self, context_id):
        if context_id in self._user_models:
            return self._user_models[context_id]
        else:
            context = self._contexts.get(context_id, [])
            models = self.sort_models(context_id, context)
            return models[0]
    
    def set_model(self, context_id, model_key):
        self._user_models[context_id] = self._model_dict[model_key]

        with open('models.pkl', 'wb') as f:
            pickle.dump(self._user_models, f)

    # @classmethod
    def trim_context(self, context :List[Dict[str,str]]):
        context_text_length = 0
        for i in range(len(context)):
            idx = i + 1
            context_text_length += len(context[-idx].text)
            if context_text_length > self._bedrock.longest_model.in_length:
                context_text_length -= len(context[-idx].text)
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
            sorted_by_price = sorted(self._bedrock.models, key=lambda x: x.in_price)
        else:
            in_length = 0
            out_length = 0
            for c in context:
                if c.type == "in":
                    in_length += len(c.text)
                else:
                    out_length += len(c.text)

            sorted_by_price = sorted(
                self._bedrock.models, key=lambda x: x.in_price * in_length + x.out_price * out_length)
        

        models = []
        if context_id in self._user_models:
            models.append(self._user_models[context_id])
            for model in sorted_by_price:
                if model.key != self._user_models[context_id].key:
                    models.append(model)
        else:
            models.extend(sorted_by_price)
        
        return models
        
class Slack():

    def __init__(self, bedrock):
        self._context_manager = ContextManager(bedrock)
        self._bedrock = bedrock

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
        current_context.append(ContextItem(text=question, type="in"))

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
            context.append(ContextItem(text=question, type="in"))
            context.append(ContextItem(text=answer["text"], type="out"))

            # save the context per channel:user
            self._context_manager.set_context(context_id, context) 

        except Exception as e:
            args.say(f"Error: {e}")
        

    def clear(self, args):
        args.ack()
        channel = args.body["channel_name"]
        user = args.body["user_name"]
        context_id = f"{channel}:{user}"
        self._context_manager.set_context(context_id, [])

        args.logger.info("Context cleared")

        args.say("Clear the context")


    def model(self, args):
        args.ack()
        channel = args.body["channel_name"]
        user = args.body["user_name"]
        context_id = f"{channel}:{user}"
        if not len(args.body["text"].strip()):
            models = self._context_manager.sort_models(context_id)
            model_name = self._context_manager.get_model(context_id).key
            args.say(f"Currently using {model_name}. You can use one of {', '.join([model.key for model in models])}")
        elif args.body["text"] in self._bedrock.model_names:
            self._bedrock.set_model(context_id, args.body["text"])
            args.say(f"Model set to {args.body['text']}")
        else:
            args.say(f"Use one of {', '.join(self._bedrock.model_names)}")


    def help(self, args):
        args.ack()
        args.say("""
    Use /llm to ask a question.
    Use /llmc to clear the context.
    Use /llmm to get the current model or to change the model.The models are sorted by price.
        """)


    def test(self, args):
        args.ack()
        body = args.body
        print(body)
        args.say(body["text"])


    def handle_files(self, args):

        args.ack()
        
        if "event" in args.body:

            event = args.body.get("event", {})

            if "files" in event:
                for file in event["files"]:

                    download_url = file.get("url_private_download")
                    print(download_url)

                    """
                    file_id = file.get("id")
                    response = args.client.files_info(file=file_id)
                    file_info = response.get("file", {})
                    download_url = file_info.get("url_private_download")
                    print(download_url)
                    """

                    headers = {
                        "Authorization": f"Bearer {args.client.token}"
                    }
                    
                    # THIS DOES NOT WORK FOR AN UNKNOWN REASON
                    # the goal was to upload files as a context
                    file_content = requests.get(download_url, headers=headers)
                    
                    """
                    if file_content.status_code > 199 and file_content.status_code < 300:
                        # print(file_content.content)
                        file_name = file.get("name", None)
                        with open(f"slack-download-{file_name}", "wb") as f:
                            f.write(file_content.content)
                        args.say(f"File '{file_name}' has been downloaded successfully!")
                    else:
                        args.say(f"Failed to download the file: {file_name}")
                    """

def slack():
    _setup_logging()
    app = App(token=os.environ.get("SLACK_BOT_TOKEN"))
    bedrock = Bedrock()
    slack = Slack(bedrock)
    app.command("/llm")(slack.ask)
    app.command("/llmc")(slack.clear)
    app.command("/llmm")(slack.model)
    app.command("/llmh")(slack.help)
    
    # app.command("/llmt")(slack.test)
    # app.event("message")(slack.handle_files)

    slack = SocketModeHandler(app, os.environ["SLACK_APP_TOKEN"])
    slack.start()

def cli():
    parser = argparse.ArgumentParser(description="A Bedrock command line tool")

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-q', action='store', help='The question to be sent to Bedrock')

    args = parser.parse_args()

    bedrock = Bedrock()
    context_manager = ContextManager(bedrock)
    context_id = "local"
    context = context_manager.get_context(context_id)

    current_context = []
    current_context.extend(context)
    if args.q:
        current_context.append(ContextItem(text=args.q, type="in"))

        # make sure that the current context does not exceed the max length
        current_context, context_text_length = context_manager.trim_context(current_context)
        
        # get the cheapest model for this channel:user
        models = context_manager.sort_models(context_id, current_context)

        try:
        
            # send the question with the context to bedrock
            answer = bedrock.call(models, current_context)
            print(answer)

            context.append(ContextItem(text=args.q, type="in"))
            context.append(ContextItem(text=answer["text"], type="out"))

            # save the context per channel:user
            context_manager.set_context(context_id, context) 

        except Exception as e:
            print(f"Error: {e}")
            return
       

if __name__ == "__main__":
    cli()