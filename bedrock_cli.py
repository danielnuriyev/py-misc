import argparse
import glob
import os

import bedrock

def cli():
    parser = argparse.ArgumentParser(description="A Bedrock command line tool")

    group = parser #.add_mutually_exclusive_group(required=True)
    group.add_argument("-a", "--ask", action="store", help="The question to be sent to Bedrock")
    group.add_argument("-c", "--clear-context", action="store_true", help="Clears the context")
    group.add_argument("-p", "--print-context", action="store_true", help="Print the context")
    group.add_argument("-m", "--list-models", action="store_true", help="Lists the models")
    group.add_argument("-s", "--set-model", action="store", help="Sets the model")
    group.add_argument("-r", "--reset-model", action="store_true", help="Resets the model")
    group.add_argument("-f", "--file-to-context", action="store", help="Uploads a file or directory as context")
    group.add_argument("-t", "--file-types", action="store", help="Comma separated list of file types such as sql,yaml,py to upload as context")
    group.add_argument("-x", "--select-files", action="store", help="Spacify criteria for files")

    args = parser.parse_args()

    bedrock_client = bedrock.Bedrock()
    context_manager = bedrock.ContextManager(bedrock_client)
    context_id = "local"
    context = context_manager.get_context(context_id)

    current_context = []
    current_context.extend(context)
    if args.ask:
        current_context.append(bedrock.ContextItem(text=args.ask, type="in"))

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

            context.append(bedrock.ContextItem(text=args.ask, type="in"))
            context.append(bedrock.ContextItem(text=answer["text"], type="out"))

            # save the context per channel:user
            context_manager.set_context(context_id, context) 

        except Exception as e:
            print(f"Error: {e}")
            return
    elif args.clear_context:
        context_manager.clear_context(context_id)
        print("Context cleared")
    elif args.print_context:
        context = context_manager.get_context(context_id)
        for item in context:
            print(item.text)
    elif args.list_models:
        model = context_manager.get_model(context_id)
        models = context_manager.sort_models(context_id, current_context)
        print(f"Using {model.key}. You can use one of {[model.key for model in models]}")
    elif args.set_model:
        if args.s in bedrock_client.model_names:
            context_manager.set_model(context_id, args.set_model)
            print(f"Model set to {args.set_model}")
        else:
            print(f"Use one of {', '.join(bedrock_client.model_names)}")
    elif args.reset_model:
        context_manager.reset_model(context_id)
        model = context_manager.get_model(context_id)
        models = context_manager.sort_models(context_id, current_context)
        print(f"Using {model.key}. You can use one of {[model.key for model in models]}")
    elif args.file_to_context:
        if os.path.isfile(args.file_to_context):
            with open(args.file_to_context, "r") as f:
                lines = f.read()
                current_context.append(bedrock.ContextItem(text=lines, type="in"))
                context_manager.set_context(context_id, current_context)
        elif os.path.isdir(args.file_to_context):
            file_paths = glob.glob(args.file_to_context + "/**/*", recursive=True)
            for file_path in file_paths:
                if os.path.isfile(file_path):

                    if args.file_types:
                        for file_type in args.file_types.split(","):
                            if not file_path.endswith(file_type):
                                continue
                                
                    with open(file_path, "r") as f:
                        lines = f.read()
                        current_context.append(bedrock.ContextItem(text=lines, type="in"))            

            context_manager.set_context(context_id, current_context)
        else:
            print(f"File {args.file_to_context} not found")

if __name__ == "__main__":
    cli()