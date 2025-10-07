import json
import boto3

def lambda_handler(event, context):
    # Initialize the Lex V2 runtime client
    lex_client = boto3.client('lexv2-runtime')

    # --- 1. Extract message and session ID from the incoming API Gateway event ---
    # The request body is a JSON string, so we need to parse it.
    body = json.loads(event.get('body', '{}'))
    user_message = body.get('messages', [{}])[0].get('unstructured', {}).get('text', '')
    session_id = body.get('messages', [{}])[0].get('unstructured', {}).get('id', 'default_session')

    # --- 2. Send the user's message to Amazon Lex ---
    # Created a new bot for this purpose in Lex V2
    # The main issue was some cached version of the bot was being used
    bot_id = 'PIWKC8MIYP'
    bot_alias_id = '5OL0EOUCT5'
    locale_id = 'en_US'

    response = lex_client.recognize_text(
        botId=bot_id,
        botAliasId=bot_alias_id,
        localeId=locale_id,
        sessionId=session_id,
        text=user_message
    )

    # --- 3. Extract the response message from Lex ---
    # The response from Lex might not have a message if it's just waiting for more input
    bot_response_message = "I'm sorry, I'm having trouble understanding. Can you repeat that?"
    if 'messages' in response and response['messages']:
        bot_response_message = response['messages'][0].get('content', bot_response_message)

    # --- 4. Format the response to send back to the frontend ---
    # The frontend expects a specific JSON structure.
    frontend_response = {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST'
        },
        'body': json.dumps({
            'messages': [{
                'type': 'unstructured',
                'unstructured': {
                    'text': bot_response_message
                }
            }]
        })
    }

    return frontend_response