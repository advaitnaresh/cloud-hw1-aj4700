import boto3
import requests
import json
import time
import datetime
from decimal import Decimal

# --- Yelp API Configuration ---
# TODO: Paste your Yelp API Key here
API_KEY = 'M2X011d1L0JrGq0LoLTaHCQnsR6Xv2FNwbCGXZttJP9h90BBtKCSH7ThGRlLdgdu3ubFIfQlssB_GprQ2IY7oX8Ev58UxZr1AXEaMMWLSBAfgggg1wPBntlkDLbkaHYx'
ENDPOINT = 'https://api.yelp.com/v3/businesses/search'
HEADERS = {'Authorization': f'bearer {API_KEY}'}

# --- AWS OpenSearch (Elasticsearch) Configuration ---
# TODO: Paste your OpenSearch domain endpoint URL here
ES_ENDPOINT = 'https://search-restaurants-qyfi2lwer4mu6iwulcpu7he4jq.us-east-1.es.amazonaws.com'
ES_INDEX = 'restaurants'
ES_TYPE = '_doc'  # For OpenSearch Service, the type is typically '_doc'
ES_USERNAME = 'admin-user'  # Replace with your OpenSearch username
ES_PASSWORD = 'AdvaitNYU*12345'  # Replace with your OpenSearch password
ES_AUTH = (ES_USERNAME, ES_PASSWORD)

# --- Define Search Parameters ---
# Define at least 5 cuisines as required by the assignment
CUISINES = ['chinese', 'japanese', 'continental', 'mexican', 'indian']  
LOCATION = 'Manhattan'
LIMIT_PER_REQUEST = 50 # Yelp returns a max of 50 results per call

# --- AWS DynamoDB Configuration ---
DYNAMODB_TABLE_NAME = 'RestaurantConciergeTableAJ4700'
# TODO: Make sure this region matches the region where you created your table
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table(DYNAMODB_TABLE_NAME)

# --- Main Scraping Logic ---
def scrape_yelp_data():
    """
    Main function to scrape data for all defined cuisines and store in DynamoDB.
    """
    unique_restaurant_ids = set()

    for cuisine in CUISINES:
        print(f"--- Scraping for cuisine: {cuisine} ---")
        # To get 200+ restaurants, we need to make multiple API calls using the 'offset' parameter.
        # This loop attempts to get up to 1000 results (Yelp's max) for the cuisine.
        for offset in range(0, 1000, LIMIT_PER_REQUEST):
            params = {
                'term': f'{cuisine} restaurants',
                'location': LOCATION,
                'limit': LIMIT_PER_REQUEST,
                'offset': offset
            }
            
            try:
                # Make the API request to Yelp
                response = requests.get(url=ENDPOINT, params=params, headers=HEADERS)
                response.raise_for_status() # Raises an exception for bad status codes
                
                businesses = response.json().get('businesses', [])
                
                if not businesses:
                    print("No more businesses found for this cuisine.")
                    break # Stop if no more businesses are returned

                for business in businesses:
                    # Check for duplicates before processing
                    if business.get('id') and business['id'] not in unique_restaurant_ids:
                        unique_restaurant_ids.add(business['id'])
                        
                        print(f"Found new restaurant: {business.get('name', 'N/A')}")

                        # Convert the raw business data, turning all floats into Decimals for DynamoDB
                        business_data_decimal = json.loads(json.dumps(business), parse_float=Decimal)

                        # Create the item dictionary to be saved in DynamoDB
                        item = {
                            'Bid': business_data_decimal.get('id'),
                            'name': business_data_decimal.get('name'),
                            'address': ', '.join(business_data_decimal.get('location', {}).get('display_address', [])),
                            'coordinates': business_data_decimal.get('coordinates'),
                            'review_count': business_data_decimal.get('review_count'),
                            'rating': business_data_decimal.get('rating'),
                            'zip_code': business_data_decimal.get('location', {}).get('zip_code'),
                            'cuisine': cuisine, # Add the cuisine type for easier searching later
                            'insertedAtTimestamp': str(datetime.datetime.now())
                        }

                        # Save the item to DynamoDB
                        table.put_item(Item=item)

                        # Prepare the document for Elasticsearch
                        es_doc = {
                            'RestaurantID': business.get('id'),
                            'Cuisine': cuisine
                        }


                        # Construct the URL for the Elasticsearch document
                        es_url = f"{ES_ENDPOINT}/{ES_INDEX}/{ES_TYPE}/{business.get('id')}"
                        es_headers = {"Content-Type": "application/json"}

                        # Send the data to Elasticsearch
                        try:
                            es_response = requests.put(
                                            es_url, 
                                            data=json.dumps(es_doc), 
                                            headers=es_headers,
                                            auth=(ES_USERNAME, ES_PASSWORD) # Add this auth parameter
                                        )
                            es_response.raise_for_status()
                        except Exception as e:
                            print(f"Error indexing to Elasticsearch: {e}")

                # Be respectful of the API rate limit
                time.sleep(0.5)

            except requests.exceptions.HTTPError as err:
                print(f"HTTP Error: {err}")
                break
            except Exception as e:
                print(f"An error occurred: {e}")
                break
            
        
        print(f"--- Scraping complete for {cuisine}! Total unique restaurants found so far: {len(unique_restaurant_ids)} ---")

if __name__ == '__main__':
    scrape_yelp_data()
    