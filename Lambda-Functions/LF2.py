import json
import boto3
import urllib.request # Using the built-in library
import base64         # Needed for handling the password
import random

# --- Configuration ---
ES_HOST = 'search-restaurants-qyfi2lwer4mu6iwulcpu7he4jq.us-east-1.es.amazonaws.com'
ES_USERNAME = 'admin-user'
ES_PASSWORD = 'AdvaitNYU*12345'
SQS_QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/584156787058/Q1'
SENDER_EMAIL = 'aj4700@nyu.edu'
DYNAMODB_TABLE_NAME = 'RestaurantConciergeTableAJ4700'

def lambda_handler(event, context):
    # --- 1. Poll SQS for a message ---
    sqs = boto3.client('sqs')
    try:
        response = sqs.receive_message(QueueUrl=SQS_QUEUE_URL, MaxNumberOfMessages=1)
        if 'Messages' not in response:
            print("No messages in queue.")
            return
            
        message = response['Messages'][0]
        receipt_handle = message['ReceiptHandle']
        message_body = json.loads(message['Body'])
        
        cuisine = message_body.get('Cuisine')
        email = message_body.get('Email')
        num_people = message_body.get('NumberOfPeople')
        dining_time = message_body.get('DiningTime')

    except Exception as e:
        print(f"Error receiving from SQS: {e}")
        return

    # --- 2. Query OpenSearch for a larger batch of restaurant IDs ---
    es_url = f"https://{ES_HOST}/restaurants/_search"
    
    # MODIFICATION: Changed "size" from 3 to 50 to get a larger pool of restaurants
    query = {"size": 50, "query": {"match": {"Cuisine": cuisine}}}
    query_bytes = json.dumps(query).encode('utf-8')

    req = urllib.request.Request(es_url, data=query_bytes, headers={'Content-Type': 'application/json'})
    
    credentials = f"{ES_USERNAME}:{ES_PASSWORD}"
    encoded_credentials = base64.b64encode(credentials.encode()).decode()
    req.add_header('Authorization', f"Basic {encoded_credentials}")
    
    try:
        with urllib.request.urlopen(req) as response:
            es_data = json.loads(response.read().decode())
            
        hits = es_data.get('hits', {}).get('hits', [])

        if not hits:
            print("No restaurants found for this cuisine.")
            return

        # MODIFICATION: Use random.sample to pick up to 3 unique suggestions from the results
        number_to_suggest = min(3, len(hits))
        random_hits = random.sample(hits, number_to_suggest)
        
        # Get the IDs from the new random list
        restaurant_ids = [hit['_source']['RestaurantID'] for hit in random_hits]

    except Exception as e:
        print(f"Error querying OpenSearch: {e}")
        return

    # --- 3. Use IDs to get details from DynamoDB ---
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(DYNAMODB_TABLE_NAME)
    
    email_message = f"Hello! Here are my {cuisine} restaurant suggestions for {num_people} people at {dining_time}:\n\n"
    for i, r_id in enumerate(restaurant_ids):
        db_response = table.get_item(Key={'Bid': r_id})
        if 'Item' in db_response:
            item = db_response['Item']
            name = item.get('name', 'N/A')
            address = item.get('address', 'N/A')
            email_message += f"{i+1}. {name}, located at {address}\n"

    email_message += "\nEnjoy your meal!"

    # --- 4. Send email using SES ---
    ses = boto3.client('ses')
    try:
        ses.send_email(
            Source=SENDER_EMAIL,
            Destination={'ToAddresses': [email]},
            Message={
                'Subject': {'Data': 'Your Dining Suggestions'},
                'Body': {'Text': {'Data': email_message}}
            }
        )
        print("Email sent successfully!")
    except Exception as e:
        print(f"Error sending email: {e}")
        return
        
    # --- 5. Delete the message from SQS ---
    try:
        sqs.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle)
    except Exception as e:
        print(f"Error deleting message from SQS: {e}")

    return {'statusCode': 200, 'body': json.dumps('Successfully processed message.')}