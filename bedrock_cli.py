import cmd2
import glob
import os
import sys

import bedrock


class Shell(cmd2.Cmd):

    intro = "A Bedrock command line tool\n"
    prompt = "➜ "

    def __init__(self):
        super().__init__()
        self.chat = bedrock.Chat("local")
    
    def do_a(self, arg):
        self.do_ask(arg)

    def do_ask(self, arg):

        arg = arg.args

        try:
        
            answer = self.chat.ask(arg)

            print()

            text = answer["text"]
            lines = text.split("\n")
            for line in lines:
                print(line)

            print()
            print(f"Context length: {answer['context_length']}")
            print(f"Model: {answer['model']}")
            print(f"Cost: ${answer['cost']:f}")
            print()

        except Exception as e:
            print(f"Error: {e}")
            return
    
    def do_cc(self, arg):
        self.do_clear_context(arg)

    def do_clear_context(self, arg):
        self.chat.clear_context()
        print("Context cleared")

    def do_pc(self, arg):
        self.do_print_context(arg)

    def do_print_context (self, arg):
        context = self.chat.get_context()
        for item in context:
            print(item.text)

    def do_cl(self, arg):
        self.do_context_length(arg)

    def do_context_length(self, arg):
        context = self.chat.get_context()
        print(sum([len(item.text) for item in context]))

    def do_ctf(self, arg):
        self.do_context_to_file(arg)

    def do_context_to_file(self, arg):
        arg = arg.args
        context = self.chat.get_context()
        txt = ""
        for item in context:
            txt += item.text
        with open(arg, "w") as f:
            f.write(txt)

    def do_lm(self, arg):
        self.do_list_models(arg)

    def do_list_models(self, arg):
        models = self.chat.list_models()    
        print(f"Using {models[0]}. You can use one of {[model for model in models]}")

    def do_sm(self, arg):
        self.do_set_model(arg)

    def do_set_model(self, arg):
        arg = arg.args
        try:
            self.chat.set_model(arg)
            print(f"Model set to {arg}")
        except Exception as e:
            print(e)

    def do_rm(self, arg):
        self.do_reset_model(arg)

    def do_reset_model(self, arg):
        self.chat.reset_model()
        self.do_list_models(arg)

    def do_ftc(self, arg):
        self.do_file_to_context(arg)

    def do_file_to_context(self, arg):
        arg = arg.args

        args = arg.split(" ")
        path = args[0]
        types = args[1] if len(args) > 1 else None

        current_context = self.chat.get_context()
        if os.path.isfile(path):
            with open(path, "r") as f:
                lines = f.read()
                current_context.append(bedrock.ContextItem(text=lines, type="in"))
                self.chat.set_context(current_context)
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

            self.chat.set_context(current_context)
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
        cl  # prints context length
        lm  # lists models
        sm {model} # sets model
        rm  # reset model to cheapest given the current context
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