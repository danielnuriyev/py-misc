import os
import pickle

from datetime import datetime
from typing import List, Dict

import boto3

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
            Model(key="meta", model_id="meta.llama3-3-70b-instruct-v1:0", in_price=0.00072, out_price=0.00072, in_length=128000, out_length=2048),
            Model(key="mistral", model_id="mistral.mistral-large-2402-v1:0", in_price=0.004, out_price=0.012, in_length=32768, out_length=8192),
            Model(key="amazon", model_id="amazon.nova-pro-v1:0", in_price=0.0008, out_price=0.0032, in_length=300000, out_length=5000),
            Model(key="cohere", model_id="cohere.command-r-plus-v1:0", in_price=0.003, out_price=0.015, in_length=128000, out_length=4096),
            Model(key="anthropic", model_id="anthropic.claude-3-7-sonnet-20250219-v1:0", in_price=0.003, out_price=0.015, in_length=200000, out_length=128000),#
            Model(key="ai21", model_id="ai21.jamba-1-5-large-v1:0", in_price=0.002, out_price=0.008, in_length=256000, out_length=256000)
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
                print(f"Model {current_model.key} failed at {datetime.now()}: {e}")

        # If all models fail, raise an HTTPException
        raise Exception("All models failed to process the request")

class ContextItem():

    def __init__(self, text, type, model=None):
        self.text = text
        self.type = type
        self.model = model

    def to_dict(self):
        return {"text": self.text, "type": self.type}

    @classmethod
    def from_dict(cls, data):
        return cls(text=data["text"], item_type=data["type"])

class Context():

    def __init__(self, bedrock):
        self._bedrock = bedrock

        if os.path.exists("contexts.pkl"):
            with open('contexts.pkl', 'rb') as f:
                self._contexts = pickle.load(f)
        else:
            self._contexts = {}

        if os.path.exists("models.pkl"):
            with open('models.pkl', 'rb') as f:
                self._user_models = pickle.load(f)
        else:   
            self._user_models = {}

        self._model_dict = {model.key: model for model in self._bedrock.models}

    def get_context(self, context_id):
        return self._contexts.get(context_id, [])
    
    def context_length(self, context_id):
        return sum([len(item.text) for item in self.get_context(context_id)])
    
    def set_context(self, context_id, context):
        context, _ = self.trim_context(context)
        self._contexts[context_id] = context

        with open('contexts.pkl', 'wb') as f:
            pickle.dump(self._contexts, f)

    def _get_model(self, context_id):
        if context_id in self._user_models:
            return self._user_models[context_id]
        else:
            context = self._contexts.get(context_id, [])
            models = self._sort_models(context_id, context)
            return models[0]
    
    def set_model(self, context_id, model_key):
        self._user_models[context_id] = self._model_dict[model_key]

        with open('models.pkl', 'wb') as f:
            pickle.dump(self._user_models, f)

    def reset_model(self, context_id):
        self._user_models = {}

        with open('models.pkl', 'wb') as f:
            pickle.dump(self._user_models, f)

    # @classmethod
    def trim_context(self, context :List[ContextItem]):
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

    def clear_context(self, context_id):
        self.set_context(context_id, [])

    def _sort_models(self, context_id, context=None):
        
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
        
    def get_models(self, context_id):
        model = self._get_model(context_id)
        current_context = self.get_context(context_id)
        models = self._sort_models(context_id, current_context)
        if model != models[0]:
            models = [model].extend([m for m in models if m != model])
        return models
    
class Chat():

    bedrock_client = Bedrock()
    context_manager = Context(bedrock_client)

    def __init__(self, context_id):
        self._context_id = context_id
    
    def ask(self, question):

        context = Chat.context_manager.get_context(self._context_id)
        current_context = []
        current_context.extend(context)

        current_context.append(ContextItem(text=question, type="in"))

        # make sure that the current context does not exceed the max length
        current_context, context_text_length = Chat.context_manager.trim_context(current_context)
        
        # get the cheapest model for this channel:user
        models = Chat.context_manager.get_models(self._context_id)

        try:
        
            # send the question with the context to bedrock
            answer = Chat.bedrock_client.call(models, current_context)

            answer["context_length"] = context_text_length

            context.append(ContextItem(text=question, type="in", model=answer["model"]))
            context.append(ContextItem(text=answer["text"], type="out", model=answer["model"]))

            # save the context per channel:user
            Chat.context_manager.set_context(self._context_id, context) 

            return answer

        except Exception as e:
            raise e
    
    def get_context(self):
        return Chat.context_manager.get_context(self._context_id)

    def clear_context(self):
        Chat.context_manager.clear_context(self._context_id)

    def context_length(self):
        return Chat.context_manager.context_length(self._context_id)
    
    def add_to_context(self, text):
        text = text.strip()
        context = Chat.context_manager.get_context(self._context_id)
        context.append(ContextItem(text, "in"))
        Chat.context_manager.set_context(self._context_id, context)

    def list_models(self):
        models = Chat.context_manager.get_models(self._context_id)    
        return [model.key for model in models]

    def set_model(self, model):
        if model in Chat.bedrock_client.model_names:
            Chat.context_manager.set_model(self._context_id, model)
        else:
            raise f"Use one of {', '.join(Chat.bedrock_client.model_names)}"

    def reset_model(self):
        Chat.context_manager.reset_model(self._context_id)