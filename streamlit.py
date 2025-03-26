import subprocess

import streamlit as st

import bedrock as br

chat = br.Chat("local")

def ask(question):
    try:
        return chat.ask(question)
    except Exception as e:
        return f"{e}"
    
messages = chat.get_context()
st.session_state.messages = []
if messages:
    st.session_state.messages.extend(messages)
    for message in messages:
        t = "question" if message.type == "in" else "answer"
        with st.chat_message(f"{t} area"):
                st.markdown(message.text)
                if t == "answer":
                    st.markdown(f":gray[model: {message.model}]")

if prompt := st.chat_input("/help for commands"):

    if prompt.startswith("/"):

        with st.chat_message("question area"):
            st.markdown(f"`{prompt}`")

        if prompt.startswith("/context reset"):
            chat.clear_context()
            print(chat.get_context())
            st.session_state.messages = []
            with st.chat_message("answer area"):
                st.markdown(f"`context cleared`")

        elif prompt.startswith("/context show"):
            messages = chat.get_context()
            for message in messages:
                t = "question" if message.type == "in" else "answer"
                with st.chat_message(f"{t} area"):
                    st.markdown(message.text)
                    if t == "answer":
                        st.markdown(f":gray[model: {message.model}]")

        elif prompt.startswith("/models"):
            models = chat.list_models()
            with st.chat_message("answer area"):
                for model in models:
                    st.markdown(f"`{model}`")

        elif prompt.startswith("/model set "):
            model = prompt.split(" ")[-1]
            chat.set_model(model)
            with st.chat_message("answer area"):
                st.markdown(f"`model set to {model}`")

        elif prompt.startswith("/model get"):
            model = chat.list_models()[0]
            with st.chat_message("answer area"):
                st.markdown(f"`current model: {model}`")

        elif prompt.startswith("/model reset"):
            chat.reset_model()
            model = chat.list_models()[0]
            with st.chat_message("answer area"):
                st.markdown(f"`model reset to: {model}`")

        elif prompt.startswith("/add "):
            prompt = prompt[len("/add "):].strip()
            chat.add_to_context(prompt)
            context_length = chat.context_length()
            with st.chat_message("question area"):
                st.markdown(f":gray[added to context:]")
                st.markdown(prompt)
                st.session_state.messages.append(prompt)
                st.markdown(f":gray[context length: {context_length}]")

        elif prompt.startswith("/bash "):
            prompt = prompt[len("/bash "):].strip()
            process = subprocess.run(
                prompt,
                shell=True,  # Important: Use shell=True for bash commands
                capture_output=True,  # Capture stdout and stderr
                text=True,  # Decode output as text (Unicode)
                check=False  # Don't raise an exception on non-zero return codes (optional)
            )
            if process.returncode == 0:
                response = process.stdout
            else:
                response = process.stderr

            with st.chat_message("answer area"):
                st.markdown(f"```{response}```")
            

        elif prompt.startswith("/status"):
            model = chat.list_models()[0]
            context_length = chat.context_length()
            with st.chat_message("answer area"):
                st.markdown(f"""
                            ```
                            model: {model}\n
                            context length: {context_length}
                            ```
                            """)

        elif prompt.startswith("/help"):
            with st.chat_message("answer area"):
                st.markdown(
                    """
                    ```
                    /add <text> \t\t- add text to the context\n
                    /context reset \t\t- clear the context\n
                    /context show \t\t- show the current context\n
                    /models \t\t- list available models\n
                    /model set <model> \t- set the model\n
                    /model get \t\t- get the current model\n
                    /model reset \t\t- reset the model\n
                    /status \t\t- show the current model and context length\n
                    /help \t\t\t- show this help message
                    ```
                    """
                )

    else:

        st.session_state.messages.append(prompt)
        with st.chat_message("question area"):
            print(prompt)
            print(prompt)
            st.markdown(prompt)

        with st.chat_message("answer area"):
            response = chat.ask(prompt)
            print("response")
            print(response) 
            st_response = st.markdown(response["text"])
            st.session_state.messages.append(st_response)
            st.markdown(f":gray[model: {response['model']}, context length: {response['context_length']}, cost: ${response['cost']:.4f}]")
