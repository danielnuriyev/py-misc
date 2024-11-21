import datetime
import logging
import os

from logging.handlers import RotatingFileHandler
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

import requests

import bedrock

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

class Slack():

    def __init__(self, bedrock):
        self._context_manager = bedrock.ContextManager(bedrock)
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
        current_context.append(bedrock.ContextItem(text=question, type="in"))

        # make sure that the current context does not exceed the max length
        current_context, context_text_length = self._context_manager.trim_context(current_context)
        
        # get the cheapest model for this channel:user
        model = self.context_manager.get_model(context_id)
        models = self._context_manager.sort_models(context_id, current_context)
        if model != models[0]:
            models = [model].extend([m for m in models if m != model])

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
            context.append(bedrock.ContextItem(text=question, type="in"))
            context.append(bedrock.ContextItem(text=answer["text"], type="out"))

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
            self._context_manager.set_model(context_id, args.body["text"])
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
    b = bedrock.Bedrock()
    slack = Slack(b)
    app.command("/llm")(slack.ask)
    app.command("/llmc")(slack.clear)
    app.command("/llmm")(slack.model)
    app.command("/llmh")(slack.help)
    
    # app.command("/llmt")(slack.test)
    # app.event("message")(slack.handle_files)

    slack = SocketModeHandler(app, os.environ["SLACK_APP_TOKEN"])
    slack.start()

if __name__ == "__main__":
    slack()