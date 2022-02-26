import json
import boto3
import datetime
import math
import time
import os
import logging
import random
from opensearchpy import OpenSearch, RequestsHttpConnection
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


"""
Reference sample codes:
    https://github.com/opensearch-project/opensearch-py
    https://docs.aws.amazon.com/ses/latest/dg/send-an-email-using-sdk-programmatically.html
"""


def SQS_pull(max_num_msg):
    """
    pull messages from the SQS queue
    """
    sqs = boto3.client('sqs')
    queue_url = "https://sqs.us-east-1.amazonaws.com/414199463068/RestaurantRequest"

    response = sqs.receive_message(
        QueueUrl=queue_url,
        AttributeNames=['All'],
        MessageAttributeNames=['All'],
        MaxNumberOfMessages=max_num_msg,
        VisibilityTimeout=0,
        WaitTimeSeconds=0
    )

    logger.debug(response)

    if 'Messages' not in response:
        print("No messages in the SQS queue.")
        return None

    return response['Messages'][0]


def delete_message_in_SQS(message):
    """
    delete the message in the SQS queue after processing
    """
    sqs = boto3.client('sqs')
    queue_url = "https://sqs.us-east-1.amazonaws.com/414199463068/RestaurantRequest"

    sqs.delete_message(
        QueueUrl=queue_url,
        ReceiptHandle=message['ReceiptHandle']
    )
    return


def get_restaurants_recommendation(cuisine, email_msg):
    """
    get a random restaurant lists from OpenSearch and DynamoDB,
    and format an email message to send
    """

    # OpenSearch search for the cuisine
    auth = ('master', '6998Cloud!')
    host = 'search-restaurants-6iggou56o64jxdkwcb7xb2iks4.us-east-1.es.amazonaws.com'

    client = OpenSearch(
        hosts=[{'host': host, 'port': 443}],
        http_auth=auth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection
    )

    query = {
        'size': 1000,
        'query': {
            'multi_match': {
                'query': cuisine
            }
        }
    }

    response = client.search(
        body=query,
        index='restaurants'
    )

    restaurant_list = response['hits']['hits']

    # generate 3 random restaurants and check info in DynamoDB
    n_restaurants = 3
    random.shuffle(restaurant_list)
    indexes = random.sample(range(0, len(restaurant_list)), n_restaurants)

    # connect to DynamoDB
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('yelp-restaurants')
    recommendations = []
    for i in range(n_restaurants):
        ind = indexes[i]
        id = restaurant_list[ind]['_source']['id']
        info = table.get_item(Key={'business_id': id})['Item']

        # Check "New York" in the restaurant address to make sure
        # location = "manhattan"
        if 'New York' not in info['address']:
            while True:
                new_ind = random.randrange(0, len(restaurant_list))
                if new_ind not in indexes:
                    indexes[i] = new_ind
                    break
            ind = indexes[i]
            id = restaurant_list[ind]['_source']['id']
            info = table.get_item(Key={'business_id': id})['Item']

        recommendations.append('{num}. {name}, located at {location}'.format(num=i+1,
                                                                             name=info['name'],
                                                                             location=info['address']))

    email_msg += ', '.join(recommendations) + '. Enjoy your meal!'

    return email_msg


def send_email(email_address, email_msg):
    """
    send an email to the user
    """
    # Replace sender@example.com with your "From" address.
    # This address must be verified with Amazon SES.
    SENDER = "Yuerong Zhang <yz4143@columbia.edu>"

    # Replace recipient@example.com with a "To" address. If your account
    # is still in the sandbox, this address must be verified.
    RECIPIENT = email_address

    # If necessary, replace us-west-2 with the AWS Region you're using for Amazon SES.
    AWS_REGION = "us-east-1"

    # The subject line for the email.
    SUBJECT = "Dining Suggestions From Chatbot"

    # The email body for recipients with non-HTML email clients.
    BODY_TEXT = email_msg

    # The character encoding for the email.
    CHARSET = "UTF-8"

    # Create a new SES resource and specify a region.
    client = boto3.client('ses', region_name=AWS_REGION)

    # Try to send the email.
    try:
        # Provide the contents of the email.
        response = client.send_email(
            Destination={
                'ToAddresses': [
                    RECIPIENT,
                ],
            },
            Message={
                'Body': {
                    'Text': {
                        'Charset': CHARSET,
                        'Data': BODY_TEXT,
                    },
                },
                'Subject': {
                    'Charset': CHARSET,
                    'Data': SUBJECT,
                },
            },
            Source=SENDER,
        )
    # Display an error if something goes wrong.
    except ClientError as e:
        print(e.response['Error']['Message'])
        return False
    else:
        print("Email sent! Message ID:"),
        print(response['MessageId'])
        return True


def lambda_handler(event, context):
    """
    main handler of events
    """
    max_num_msg = 1
    message = SQS_pull(max_num_msg)
    if message is None:
        return {
            'statusCode': 200,
            'body': 'No message found in the SQS queue.'
        }

    # get info from the message
    location = message["MessageAttributes"]["Location"]["StringValue"]
    cuisine = message["MessageAttributes"]["Cuisine"]["StringValue"]
    num_of_people = message["MessageAttributes"]["NumberOfPeople"]["StringValue"]
    date = message["MessageAttributes"]["Date"]["StringValue"]
    time = message["MessageAttributes"]["Time"]["StringValue"]
    email = message["MessageAttributes"]["Email"]["StringValue"]

    # delete the message in the SQS queue after processing
    delete_message_in_SQS(message)

    if cuisine is None or email is None:
        return {
            'statusCode': 200,
            'body': 'Cuisine or email address missing in the message.'
        }

    # get a random restaurant recommendation for the cuisine
    # and format an email message to send
    email_msg = 'Hello! Here are my {cuisine} restaurant suggestions for {num_of_people} people, ' \
                'for {date} at {time}: '.format(cuisine=cuisine,
                                                num_of_people=num_of_people,
                                                date=date,
                                                time=time)
    email_msg = get_restaurants_recommendation(cuisine, email_msg)

    # send the email message to the user's email address
    sent_status = send_email(email, email_msg)
    if not sent_status:
        return {
            'statusCode': 200,
            'body': 'Error in sending email.'
        }

    return {
        'statusCode': 200,
        'body': 'Finished LF2.'
    }

