# -*- coding: utf-8 -*-
from __future__ import print_function

"""
Reference sample codes:
    https://github.com/Yelp/yelp-fusion/blob/master/fusion/python/sample.py
    https://github.com/opensearch-project/opensearch-py
    https://docs.aws.amazon.com/opensearch-service/latest/developerguide/integrations.html#integrations-dynamodb
"""

import argparse
import json
import pprint
import requests
from requests_aws4auth import AWS4Auth
import sys
import urllib
import datetime
import boto3
import botocore
from decimal import Decimal
import time
from opensearchpy import OpenSearch, RequestsHttpConnection


# This client code can run on Python 2.x or 3.x.  Your imports can be
# simpler if you only need one of those.
try:
    # For Python 3.0 and later
    from urllib.error import HTTPError
    from urllib.parse import quote
    from urllib.parse import urlencode
except ImportError:
    # Fall back to Python 2's urllib2 and urllib
    #from urllib2 import HTTPError
    from urllib import quote
    from urllib import urlencode

# Yelp Fusion no longer uses OAuth as of December 7, 2017.
# You no longer need to provide Client ID to fetch Data
# It now uses private keys to authenticate requests (API Key)
# You can find it on
# https://www.yelp.com/developers/v3/manage_app
API_KEY = "oHcFm1HfmJjLgxMrwtvEx9KubZ2px7sHTw9cRQluWoiqciM_-tRG9Tnp6ZQGF6tSXcUpH5sVCW7Gar858a-8BH_LPqAN_efKwLQd3-f06ugcmDmtLycXjRjQxSEVYnYx"

# API constants, you shouldn't have to change these.
API_HOST = 'https://api.yelp.com'
SEARCH_PATH = '/v3/businesses/search'
BUSINESS_PATH = '/v3/businesses/'  # Business ID will come after slash.

# Defaults for our simple example.
# DEFAULT_TERM = 'dinner'
# DEFAULT_LOCATION = 'San Francisco, CA'
SEARCH_LIMIT = 50


def request(host, path, api_key, url_params=None):
    """Given your API_KEY, send a GET request to the API.

    Args:
        host (str): The domain host of the API.
        path (str): The path of the API after the domain.
        API_KEY (str): Your API Key.
        url_params (dict): An optional set of query parameters in the request.

    Returns:
        dict: The JSON response from the request.

    Raises:
        HTTPError: An error occurs from the HTTP request.
    """
    url_params = url_params or {}
    url = '{0}{1}'.format(host, quote(path.encode('utf8')))
    headers = {
        'Authorization': 'Bearer %s' % api_key,
    }

    print(u'Querying {0} ...'.format(url))

    response = requests.request('GET', url, headers=headers, params=url_params)

    return response.json()


def search(api_key, term, location, offset):
    """Query the Search API by a search term and location.

    Args:
        term (str): The search term passed to the API.
        location (str): The search location passed to the API.

    Returns:
        dict: The JSON response from the request.
    """

    url_params = {
        'term': term.replace(' ', '+'),
        'location': location.replace(' ', '+'),
        'limit': SEARCH_LIMIT,
        'offset': offset
    }
    return request(API_HOST, SEARCH_PATH, api_key, url_params=url_params)


def get_business(api_key, business_id):
    """Query the Business API by a business ID.

    Args:
        business_id (str): The ID of the business to query.

    Returns:
        dict: The JSON response from the request.
    """
    business_path = BUSINESS_PATH + business_id

    return request(API_HOST, business_path, api_key)


def query_api(term, location, offset):
    """Queries the API by the input values from the user.

    Args:
        term (str): The search term to query.
        location (str): The location of the business to query.
    """
    response = search(API_KEY, term, location, offset)
    businesses = response.get('businesses')

    return businesses


def yelp_scrape(file_path):
    """
    Scrape restaurants from yelp and output to a JSON file
    """
    cuisine_types = ['chinese', 'japanese', 'thai', 'italian', 'american', 'mexican', 'vietnamese', 'korean']
    location = "Manhattan, NY"
    restaurants = dict()

    start_time = time.time()
    print("---------- Start scraping restaurants from yelp ----------\n")

    # scrape data from yelp
    try:
        for cuisine in cuisine_types:
            term = cuisine + ' restaurants'
            print(term)
            count = 0
            for offset in range(0, 1401, 50):
                businesses = query_api(term, location, offset)
                # if no businesses found, search for the next cuisine
                if not businesses:
                    break
                count += len(businesses)
                # add non-duplicate businesses to restaurants
                for business in businesses:
                    business_id = business['id']
                    if business_id not in restaurants:
                        business['cuisine'] = [cuisine]
                        restaurants[business_id] = business
                    else:
                        restaurants[business_id]['cuisine'].append(cuisine)
            print('Total {} businesses found\n'.format(count))
        print('Total {} restaurants found\n'.format(len(restaurants.keys())))
    except HTTPError as error:
        sys.exit(
            'Encountered HTTP error {0} on {1}:\n {2}\nAbort program.'.format(
                error.code,
                error.url,
                error.read(),
            )
        )

    time_elapsed = time.time() - start_time
    print("\nScraping data runs {:.0f}m {:.0f}s".format(time_elapsed//60, time_elapsed%60))
    print("---------- End scraping restaurants from yelp ----------\n")

    # output restaurants into a JSON file
    json_str = json.dumps(restaurants, indent=4)
    with open(file_path, 'w') as json_file:
        json_file.write(json_str)


def DynamoDB_store(file_path):
    """
    Store the scraped restaurants in DynamoDB
    """
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('yelp-restaurants')

    with open(file_path) as json_file:
        restaurants = json.load(json_file)

        start_time = time.time()
        print("---------- Start uploading restaurants data to DynamoDB ----------\n")

        for i, r in restaurants.items():
            response = table.put_item(
                Item={
                    'business_id': r['id'],
                    'name': r['name'],
                    'address': ', '.join(r['location']['display_address']),
                    'coordinates': {'latitude': str(r['coordinates']['latitude']),
                                    'longitude': str(r['coordinates']['longitude'])},
                    'num_of_reviews': r['review_count'],
                    'rating': Decimal(r['rating']),
                    'zip_code': r['location']['zip_code'],
                    'cuisine': r['cuisine'],
                    'inserted_at_timestamp': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
            )

        time_elapsed = time.time() - start_time
        print("\nUploading data to DynamoDB runs {:.0f}m {:.0f}s".format(time_elapsed // 60, time_elapsed % 60))
        print("---------- End uploading restaurants to DynamoDB ----------\n")


def OpenSearch_store(file_path):
    """
    Store partial information for each restaurant scraped in OpenSearch
    """

    auth = ('master', '6998Cloud!')
    host = 'search-restaurants-6iggou56o64jxdkwcb7xb2iks4.us-east-1.es.amazonaws.com'

    client = OpenSearch(
        hosts=[{'host': host, 'port': 443}],
        http_auth=auth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection
    )

    with open(file_path) as json_file:
        restaurants = json.load(json_file)

        start_time = time.time()
        print("---------- Start uploading restaurants data to OpenSearch ----------\n")

        for i, r in restaurants.items():
            restaurant = {
                'id': r['id'],
                'cuisine': r['cuisine']
            }

            response = client.index(
                index='restaurants',
                id=r['id'],
                body=json.dumps(restaurant),
                refresh=True)

        time_elapsed = time.time() - start_time
        print("\nUploading data to DynamoDB runs {:.0f}m {:.0f}s".format(time_elapsed // 60, time_elapsed % 60))
        print("---------- End uploading restaurants to OpenSearch ----------\n")

        # ['chinese', 'american']
        # print(client.get(index='restaurants', id='XsXLVWr1UZWVhKThNvNiaA'))


if __name__ == '__main__':

    file_path = 'restaurants_info.json'

    yelp_scrape(file_path)

    DynamoDB_store(file_path)

    OpenSearch_store(file_path)

