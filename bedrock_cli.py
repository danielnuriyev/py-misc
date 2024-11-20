import argparse
import cmd
import glob
import os
import sys

import bedrock

class Shell(cmd.Cmd):

    intro = "A Bedrock command line tool\n"
    prompt = "➜ "

    """
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
    """

    bedrock_client = bedrock.Bedrock()
    context_manager = bedrock.ContextManager(bedrock_client)
    context_id = "local"
    
    def do_ask(self, arg):

        context = Shell.context_manager.get_context(Shell.context_id)
        current_context = []
        current_context.extend(context)

        current_context.append(bedrock.ContextItem(text=arg, type="in"))

        # make sure that the current context does not exceed the max length
        current_context, context_text_length = Shell.context_manager.trim_context(current_context)
        
        # get the cheapest model for this channel:user
        models = Shell.context_manager.sort_models(Shell.context_id, current_context)

        try:
        
            # send the question with the context to bedrock
            answer = Shell.bedrock_client.call(models, current_context)
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
            print()

            context.append(bedrock.ContextItem(text=arg, type="in"))
            context.append(bedrock.ContextItem(text=answer["text"], type="out"))

            # save the context per channel:user
            Shell.context_manager.set_context(Shell.context_id, context) 

        except Exception as e:
            print(f"Error: {e}")
            return
    
    
    def do_clear_context(self, arg):
        Shell.context_manager.clear_context(Shell.context_id)
        print("Context cleared")

    def do_print_context(self, arg):
        context = Shell.context_manager.get_context(Shell.context_id)
        for item in context:
            print(item.text)

    def do_context_to_file(self, arg):
        context = Shell.context_manager.get_context(Shell.context_id)
        txt = ""
        for item in context:
            txt += item.text
        with open(arg, "w") as f:
            f.write(txt)

    def do_list_models(self, arg):
        model = Shell.context_manager.get_model(Shell.context_id)
        current_context = Shell.context_manager.get_context(Shell.context_id)
        models = Shell.context_manager.sort_models(Shell.context_id, current_context)
        print(f"Using {model.key}. You can use one of {[model.key for model in models]}")

    def do_set_model(self, arg):
        if arg in Shell.bedrock_client.model_names:
            Shell.context_manager.set_model(Shell.context_id, arg)
            print(f"Model set to {arg}")
        else:
            print(f"Use one of {', '.join(Shell.bedrock_client.model_names)}")

    def do_reset_model(self, arg):
        Shell.context_manager.reset_model(Shell.context_id)
        model = Shell.context_manager.get_model(Shell.context_id)
        print(model.key)
        current_context = Shell.context_manager.get_context(Shell.context_id)
        models = Shell.context_manager.sort_models(Shell.context_id, current_context)
        print(f"Using {model.key}. You can use one of {[model.key for model in models]}")

    def do_file_to_context(self, arg):
        current_context = Shell.context_manager.get_context(Shell.context_id)
        if os.path.isfile(arg):
            with open(arg, "r") as f:
                lines = f.read()
                current_context.append(bedrock.ContextItem(text=lines, type="in"))
                Shell.context_manager.set_context(Shell.context_id, current_context)
        elif os.path.isdir(arg):
            file_paths = glob.glob(arg + "/**/*", recursive=True)
            for file_path in file_paths:
                if os.path.isfile(file_path):

                    """
                    if args.file_types:
                        for file_type in args.file_types.split(","):
                            if not file_path.endswith(file_type):
                                continue
                    """            
                    with open(file_path, "r") as f:
                        lines = f.read()
                        current_context.append(bedrock.ContextItem(text=lines, type="in"))            

            Shell.context_manager.set_context(Shell.context_id, current_context)
        else:
            print(f"File {arg} not found")

    def do_quit(self, arg):
        """Exit the shell - Usage: quit"""
        print("Goodbye!")
        return True

if __name__ == "__main__":
    try:
        shell = Shell()
        shell.cmdloop()
    except KeyboardInterrupt:
        print("\nGoodbye!")
        sys.exit(0)