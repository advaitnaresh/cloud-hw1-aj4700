import json
import boto3

def close(session_attributes, intent_name, message):
    """
    Builds a response to send back to Lex to close the conversation.
    """
    return {
        'sessionState': {
            'sessionAttributes': session_attributes,
            'dialogAction': {
                'type': 'Close'
            },
            'intent': {
                'name': intent_name,
                'state': 'Fulfilled'
            }
        },
        'messages': [message]
    }

def lambda_handler(event, context):
    """
    This is the main entry point for the Lambda function.
    """
    # Print the event for debugging purposes in CloudWatch
    print(json.dumps(event))
    
    # Get the name of the intent from the event
    intent_name = event['sessionState']['intent']['name']
    session_attributes = event['sessionState'].get('sessionAttributes', {})

    # --- Intent Dispatcher ---
    
    if intent_name == 'GreetingIntent':
        # Handle the GreetingIntent
        # 1. Create a message object.
        message = {'contentType': 'PlainText', 'content': 'Hi there, how can I assist you today?'}
        # 2. Return a 'close' response with the message.
        return close(session_attributes, 'GreetingIntent', message)

    elif intent_name == 'ThankYouIntent':
        # Handle the ThankYouIntent
        # 1. Create a message object.
        message = {'contentType': 'PlainText', 'content': 'You are most welcome :)'}
        # 2. Return a 'close' response with the message.
        return close(session_attributes, 'ThankYouIntent', message)
        
    elif intent_name == 'DiningSuggestionsIntent':
        # This is where the main logic for collecting slot data and sending to SQS goes.
        slots = event['sessionState']['intent']['slots']
        
        # Check if all slots have been filled. 'invocationSource' will be 'FulfillmentCodeHook'
        # when Lex believes the intent is ready to be fulfilled.
        if event['invocationSource'] == 'FulfillmentCodeHook':
        
            # Push the collected slot information to SQS
            # 1. Get the SQS queue URL. You will need to look this up in the SQS console.
            sqs_queue_url = 'https://sqs.us-east-1.amazonaws.com/584156787058/Q1'
            
            # 2. Extract slot values.
            cuisine = slots['Cuisine']['value']['interpretedValue']
            location = slots['Location']['value']['interpretedValue']
            num_people = slots['NumberOfPeople']['value']['interpretedValue']
            dining_time = slots['DiningTime']['value']['interpretedValue']
            email = slots['Email']['value']['interpretedValue']

            # 3. Create a boto3 client for SQS.
            sqs = boto3.client('sqs')

            # 4. Send the message to SQS.
            try:
                sqs.send_message(
                    QueueUrl=sqs_queue_url,
                    MessageBody=json.dumps({
                        'Cuisine': cuisine,
                        'Location': location,
                        'NumberOfPeople': num_people,
                        'DiningTime': dining_time,
                        'Email': email
                    })
                )
            except Exception as e:
                print(f"Error sending to SQS: {e}")
                # Handle the error appropriately

            # 5. Formulate the confirmation message to the user.
            confirmation_message = f"Thank you for all the information. The recommended suggestion for {cuisine} food in {location} for {num_people} people at {dining_time} will be in your inbox shortly! Have a nice day! Thank you for using Advait's Chatbot :)"
            message = {'contentType': 'PlainText', 'content': confirmation_message}
            
            # 6. Return a 'close' response to Lex.
            return close(session_attributes, 'DiningSuggestionsIntent', message)
            
    # If the intent is not recognized, you can provide a fallback response.
    # This part is not strictly required by the assignment but is good practice.
    message = {'contentType': 'PlainText', 'content': "Sorry not a valid answer/question. Can you please try again?"}
    return close(session_attributes, 'FallbackIntent', message)