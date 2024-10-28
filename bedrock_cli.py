import argparse
import glob
import os

import bedrock

def cli():
    parser = argparse.ArgumentParser(description="A Bedrock command line tool")

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-a', action='store', help='The question to be sent to Bedrock')
    group.add_argument('-c', action='store_true', help='Clears the context')
    group.add_argument('-m', action='store_true', help='Lists the models')
    group.add_argument('-s', action='store', help='Sets the model')
    group.add_argument('-r', action='store_true', help='Resets the model')
    group.add_argument('-f', action='store', help='Uploads a file as context')

    args = parser.parse_args()

    bedrock_client = bedrock.Bedrock()
    context_manager = bedrock.ContextManager(bedrock_client)
    context_id = "local"
    context = context_manager.get_context(context_id)

    current_context = []
    current_context.extend(context)
    if args.a:
        current_context.append(bedrock.ContextItem(text=args.a, type="in"))

        # make sure that the current context does not exceed the max length
        current_context, context_text_length = context_manager.trim_context(current_context)
        
        # get the cheapest model for this channel:user
        models = context_manager.sort_models(context_id, current_context)

        try:
        
            # send the question with the context to bedrock
            answer = bedrock_client.call(models, current_context)
            # answer["context_length"] = context_text_length

            print()

            text = answer["text"]
            lines = text.split("\n")
            for line in lines:
                print(line)

            print()
            print(f"Context length: {context_text_length}")
            print(f"Model: {answer['model']}")
            print(f"Cost: ${answer['cost']:f}")

            context.append(bedrock.ContextItem(text=args.a, type="in"))
            context.append(bedrock.ContextItem(text=answer["text"], type="out"))

            # save the context per channel:user
            context_manager.set_context(context_id, context) 

        except Exception as e:
            print(f"Error: {e}")
            return
    elif args.c:
        context_manager.clear_context(context_id)
        print("Context cleared")
    elif args.m:
        model = context_manager.get_model(context_id)
        models = context_manager.sort_models(context_id, current_context)
        print(f"Using {model.key}. You can use one of {[model.key for model in models]}")
    elif args.s:
        if args.s in bedrock_client.model_names:
            context_manager.set_model(context_id, args.s)
            print(f"Model set to {args.s}")
        else:
            args.say(f"Use one of {', '.join(bedrock_client.model_names)}")
    elif args.r:
        context_manager.reset_model(context_id)
        model = context_manager.get_model(context_id)
        models = context_manager.sort_models(context_id, current_context)
        print(f"Using {model.key}. You can use one of {[model.key for model in models]}")
    elif args.f:
        if os.path.isfile(args.f):
            with open(args.f, "r") as f:
                lines = f.read()
                current_context.append(bedrock.ContextItem(text=lines, type="in"))
                context_manager.set_context(context_id, current_context)
        elif os.path.isdir(args.f):
            file_paths = glob.glob(args.f + "/**/*", recursive=True)
            for file_path in file_paths:
                if os.path.isfile(file_path):
                    with open(file_path, "r") as f:
                        lines = f.read()
                        current_context.append(bedrock.ContextItem(text=lines, type="in"))            
            context_manager.set_context(context_id, current_context)
        else:
            print(f"File {args.f} not found")

if __name__ == "__main__":
    cli()