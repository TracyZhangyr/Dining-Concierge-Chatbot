import json
import boto3

# Define the client to interact with Lex
client = boto3.client('lex-runtime')


def lambda_handler(event, context):

    msg_from_user = event['messages'][0]['unstructured']['text']

    print(f"Message from frontend: {msg_from_user}")

    response = client.post_text(botName='DiningConcierge',
                                botAlias='dine',
                                userId='testuser',
                                inputText=msg_from_user)
    
    msg_from_lex = response['message']
    if msg_from_lex:
        print(f"Message from Chatbot: {msg_from_lex}")
        print(response)

        resp = {
            'statusCode': 200,
            'messages':
                [
                    {
                        'type': 'unstructured',
                        'unstructured':
                            {
                                'text': msg_from_lex
                            }
                    }
                ]
            }

        return resp

