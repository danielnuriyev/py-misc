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

        if os.path.exists("models.pkl"):
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

    def reset_model(self, context_id):
        del self._user_models[context_id]

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

    def clear_context(self, context_id):
        self.set_context(context_id, [])

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
        