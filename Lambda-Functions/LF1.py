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

def delegate(session_attributes, intent_name, slots):
    """
    Builds a response to tell Lex to continue eliciting slots.
    """
    return {
        'sessionState': {
            'sessionAttributes': session_attributes,
            'dialogAction': {
                'type': 'Delegate'
            },
            'intent': {
                'name': intent_name,
                'slots': slots
            }
        }
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
        message = {'contentType': 'PlainText', 'content': 'Hi there, how can I assist you today?'}
        return close(session_attributes, 'GreetingIntent', message)

    elif intent_name == 'ThankYouIntent':
        message = {'contentType': 'PlainText', 'content': 'You are most welcome :)'}
        return close(session_attributes, 'ThankYouIntent', message)
        
    elif intent_name == 'DiningSuggestionsIntent':
        slots = event['sessionState']['intent']['slots']
        
        # This part runs AFTER all slots are filled (Fulfillment)
        if event['invocationSource'] == 'FulfillmentCodeHook':
            sqs_queue_url = 'https://sqs.us-east-1.amazonaws.com/584156787058/Q1'
            
            cuisine = slots.get('Cuisine', {}).get('value', {}).get('interpretedValue')
            location = slots.get('Location', {}).get('value', {}).get('interpretedValue')
            num_people = slots.get('NumberOfPeople', {}).get('value', {}).get('interpretedValue')
            dining_time = slots.get('DiningTime', {}).get('value', {}).get('interpretedValue')
            email = slots.get('Email', {}).get('value', {}).get('interpretedValue')
            
            sqs = boto3.client('sqs')
            try:
                sqs.send_message(
                    QueueUrl=sqs_queue_url,
                    MessageBody=json.dumps({
                        'Cuisine': cuisine, 'Location': location,
                        'NumberOfPeople': num_people, 'DiningTime': dining_time,
                        'Email': email
                    })
                )
            except Exception as e:
                print(f"Error sending to SQS: {e}")

            confirmation_message = f"Thank you for all the information. The recommended suggestion for {cuisine} food in {location} for {num_people} people at {dining_time} will be in your inbox shortly! Have a nice day! Thank you for using Advait's Chatbot :)"
            message = {'contentType': 'PlainText', 'content': confirmation_message}
            
            return close(session_attributes, 'DiningSuggestionsIntent', message)
        
        # This part runs DURING the conversation to let Lex ask the questions
        else:
            return delegate(session_attributes, intent_name, slots)
    
    # Fallback for any other intent
    else:
        message = {'contentType': 'PlainText', 'content': "Sorry not a valid answer/question. Can you please try again?"}
        return close(session_attributes, 'FallbackIntent', message)