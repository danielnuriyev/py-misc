
import json

import boto3

session = boto3.Session()
client = session.client('lambda')

def ask(text, model="ai21"):
    
    data = {
        "body": json.dumps({"text": text, "model": model})
    }

    response = client.invoke(
        FunctionName="arn:aws:lambda:us-east-1:875029979081:function:bedrock-api-2",
        InvocationType='RequestResponse',
        Payload=json.dumps(data)
    )

    print(response)

    return json.loads(response['Payload'].read().decode('utf-8'))["body"]

def main():

    history = []

    while True:
        
        q = input("Enter the question:").strip()
        if q in ("exit", "quit","stop","bye"):
            break
        
        m = input("Enter the model (enter for ai21):").strip()
        if m not in ["ai21", "anthropic", "cohere", "amazon", "mistral", "meta"]:
            m = "ai21"

        context = ""
        if len(history) > 0:
            context = "Use this as the context:/n/n" + "/n/n".join(history)

        context += "/n/nUsing the context above, answer this question:/n/n" + q

        history.append(q)

        response = ask(q, m)
        print(response)

        history.append(response)
    

if __name__ == '__main__':
    main()

