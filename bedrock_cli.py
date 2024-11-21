import argparse
import cmd2
import glob
import os
import sys

import bedrock

class Shell(cmd2.Cmd):

    intro = "A Bedrock command line tool\n"
    prompt = "➜ "

    bedrock_client = bedrock.Bedrock()
    context_manager = bedrock.ContextManager(bedrock_client)
    context_id = "local"
    
    def do_a(self, arg):
        self.do_ask(arg)

    def do_ask(self, arg):

        arg = arg.args

        context = Shell.context_manager.get_context(Shell.context_id)
        current_context = []
        current_context.extend(context)

        current_context.append(bedrock.ContextItem(text=arg, type="in"))

        # make sure that the current context does not exceed the max length
        current_context, context_text_length = Shell.context_manager.trim_context(current_context)
        
        # get the cheapest model for this channel:user
        models = Shell.context_manager.get_models(Shell.context_id)

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
    
    def do_cc(self, arg):
        self.do_clear_context(arg)

    def do_clear_context(self, arg):
        Shell.context_manager.clear_context(Shell.context_id)
        print("Context cleared")

    def do_pc(self, arg):
        self.do_print_context(arg)

    def do_print_context (self, arg):
        context = Shell.context_manager.get_context(Shell.context_id)
        for item in context:
            print(item.text)

    def do_cl(self, arg):
        self.do_context_length(arg)

    def do_context_length(self, arg):
        context = Shell.context_manager.get_context(Shell.context_id)
        print(sum([len(item.text) for item in context]))

    def do_ctf(self, arg):
        self.do_context_to_file(arg)

    def do_context_to_file(self, arg):
        arg = arg.args
        context = Shell.context_manager.get_context(Shell.context_id)
        txt = ""
        for item in context:
            txt += item.text
        with open(arg, "w") as f:
            f.write(txt)

    def do_lm(self, arg):
        self.do_list_models(arg)

    def do_list_models(self, arg):
        models = Shell.context_manager.get_models(Shell.context_id)    
        print(f"Using {models[0].key}. You can use one of {[model.key for model in models]}")

    def do_sm(self, arg):
        self.do_set_model(arg)

    def do_set_model(self, arg):
        arg = arg.args
        if arg in Shell.bedrock_client.model_names:
            Shell.context_manager.set_model(Shell.context_id, arg)
            print(f"Model set to {arg}")
        else:
            print(f"Use one of {', '.join(Shell.bedrock_client.model_names)}")

    def do_rm(self, arg):
        self.do_reset_model(arg)

    def do_reset_model(self, arg):
        Shell.context_manager.reset_model(Shell.context_id)
        self.do_list_models(arg)

    def do_ftc(self, arg):
        self.do_file_to_context(arg)

    def do_file_to_context(self, arg):
        arg = arg.args

        args = arg.split(" ")
        path = args[0]
        types = args[1] if len(args) > 1 else None

        current_context = Shell.context_manager.get_context(Shell.context_id)
        if os.path.isfile(path):
            with open(path, "r") as f:
                lines = f.read()
                current_context.append(bedrock.ContextItem(text=lines, type="in"))
                Shell.context_manager.set_context(Shell.context_id, current_context)
        elif os.path.isdir(path):
            pattern = "*"
            if types:
                _types = types.split(",")
                if len(_types) == 1:
                    pattern = f"*.{_types[0]}"
                else:
                    pattern = f"*.{{types}}"

            file_paths = glob.glob(path + f"/**/{pattern}", recursive=True)
            for file_path in file_paths:
                if os.path.isfile(file_path):
                    
                    with open(file_path, "r") as f:
                        lines = f.read()
                        current_context.append(bedrock.ContextItem(text=lines, type="in"))            

            Shell.context_manager.set_context(Shell.context_id, current_context)
        else:
            print(f"File {path} not found")

    def do_q(self, arg):
        return self.do_quit(arg)

    def do_quit(self, arg):
        return True
    
    def do_help(self, arg):
        help = """
        a {question}
        cc  # clears context
        pc  # prints context
        ctf {file path}  # writes context to a text file
        lm  # lists models
        sm {model} # sets model
        rm  # reset model to default
        ftc {path} {extention:options} # adds a file or files in a directory to the context
            ftc /full/path/to/file.txt
            ftc /full/path/to/directory
            ftc /full/path/to/directory yaml
            ftc /full/path/to/directory yaml,sql
        q  # quit
        """
        print(help) 

if __name__ == "__main__":
    try:
        Shell().cmdloop()
    except KeyboardInterrupt:
        sys.exit(0)