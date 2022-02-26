import json
import boto3
import dateutil.parser
import datetime
import math
import time
import os
import re
import logging

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

"""
Reference sample codes:
    https://github.com/amazon-archives/serverless-app-examples/blob/master/python/lex-order-flowers-python/lambda_function.py
    https://github.com/amazon-archives/serverless-app-examples/blob/master/python/lex-book-trip-python/lambda_function.py
    https://github.com/amazon-archives/serverless-app-examples/blob/master/python/lex-make-appointment-python/lambda_function.py
"""

""" --- Helpers to build responses  --- """


def close(session_attributes, fulfillment_state, message):
    """
    make 'Close' type messages
    """
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Close',
            'fulfillmentState': fulfillment_state,
            'message': message
        }
    }


def delegate(session_attributes, slots):
    """
    make 'Delegate' type message
    """
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Delegate',
            'slots': slots
        }
    }


def elicit_slot(session_attributes, intent_name, slots, slot_to_elicit, message):
    """
    message for elicit slots
    """
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ElicitSlot',
            'intentName': intent_name,
            'slots': slots,
            'slotToElicit': slot_to_elicit,
            'message': message
        }
    }


def get_slots(intent_request):
    """
    get current intent's slots
    """
    return intent_request['currentIntent']['slots']


""" --- Helper Functions --- """


def build_validation_result(is_valid, violated_slot, message_content):
    if message_content is None:
        return {
            "isValid": is_valid,
            "violatedSlot": violated_slot,
        }

    return {
        'isValid': is_valid,
        'violatedSlot': violated_slot,
        'message': {'contentType': 'PlainText',
                    'content': message_content
                    }
    }


def isvalid_date(date):
    try:
        dateutil.parser.parse(date)
        return True
    except ValueError:
        return False


def parse_int(n):
    try:
        return int(n)
    except ValueError:
        return float('nan')


def validate_dining_suggestions(location, cuisine, num_of_people,
                                date, time, email):
    """
    make validations on DiningSuggestionsIntent input data
    """

    # check location
    locations = ['manhattan']
    if location is not None and location.lower() not in locations:
        return build_validation_result(
            False,
            'Location',
            'We currently do not support suggestions for restaurant in {}. Could you try Manhattan?'.format(location))

    # check cuisine types
    cuisine_types = ['chinese', 'japanese', 'thai', 'italian', 'american', 'mexican', 'vietnamese', 'korean']
    if cuisine is not None and cuisine.lower() not in cuisine_types:
        return build_validation_result(
            False,
            'Cuisine',
            'We currently do not support suggestions for {} restaurants. '.format(cuisine) +
            'Please choose one from the following: Chinese, Japanese, Thai, Italian, American, Mexican, Vietnamese, '
            'Korean. '
        )

    # check number of people
    # 1. int number >= 1
    if num_of_people is not None:
        if not num_of_people.isnumeric() or int(num_of_people) < 1:
            return build_validation_result(
                False,
                'NumberOfPeople',
                'Please enter a valid number for people.'
            )

    # check date
    # 1. valid date
    # 2. date not in the past
    if date is not None:
        if not isvalid_date(date):
            return build_validation_result(
                False,
                'Date',
                'Please enter a valid date in the format: yyyy-mm-dd.'
            )
        elif datetime.datetime.strptime(date, '%Y-%m-%d').date() < datetime.date.today():
            return build_validation_result(
                False,
                'Date',
                'This date is in the past. Please enter a date from today onwards.'
            )

    # check time
    # 1. valid time
    # 2. 7:00 - 24:00 open hours
    if time is not None:
        if len(time) != 5:
            return build_validation_result(False, 'Time', None)

        hour, minute = time.split(':')
        hour = parse_int(hour)
        minute = parse_int(minute)
        if math.isnan(hour) or math.isnan(minute):
            return build_validation_result(False, 'Time', None)

        if 0 < hour < 7:
            return build_validation_result(
                False,
                'Time',
                'Please enter a time between the open hours from 7:00 to 24:00.'
            )

    return build_validation_result(True, None, None)


""" --- Functions that control the bot's behavior --- """


def greeting(intent_request):
    """
    GreetingIntent
    """
    return close(intent_request['sessionAttributes'],
                 'Fulfilled',
                 {
                     "contentType": "PlainText",
                     "content": "Hi, what can I assist you today?"
                 })


def thank_you(intent_request):
    """
    ThankYouIntent
    """
    return close(intent_request['sessionAttributes'],
                 'Fulfilled',
                 {
                     "contentType": "PlainText",
                     "content": "You’re welcome. Have a good day!"
                 })


def dining_suggestions(intent_request):
    """
    DiningSuggestionsIntent
    """
    # get the slots' info
    location = get_slots(intent_request)['Location']
    cuisine = get_slots(intent_request)['Cuisine']
    num_of_people = get_slots(intent_request)['NumberOfPeople']
    date = get_slots(intent_request)['Date']
    time = get_slots(intent_request)['Time']
    email = get_slots(intent_request)['Email']
    source = intent_request['invocationSource']

    if source == 'DialogCodeHook':
        # validate inputs
        slots = get_slots(intent_request)
        #logger.debug(slots)
        validation_result = validate_dining_suggestions(location, cuisine, num_of_people,
                                                        date, time, email)

        # Not valid inputs
        if not validation_result['isValid']:
            slots[validation_result['violatedSlot']] = None
            return elicit_slot(intent_request['sessionAttributes'],
                               intent_request['currentIntent']['name'],
                               slots,
                               validation_result['violatedSlot'],
                               validation_result['message'])

        output_session_attributes = intent_request['sessionAttributes']
        return delegate(output_session_attributes, get_slots(intent_request))

    # push the valid info to an SQS queue
    SQS_send(get_slots(intent_request))

    # request received confirmation message
    return close(intent_request['sessionAttributes'],
                 'Fulfilled',
                 {
                     "contentType": "PlainText",
                     "content": "Thank you, and you're all set. Expect my suggestions shortly!"
                 })


""" --- SQS queues --- """


def SQS_send(data):
    """
    push the collected info from the user to an SQS queue
    """
    sqs = boto3.client('sqs')

    queue_url = "https://sqs.us-east-1.amazonaws.com/414199463068/RestaurantRequest"

    response = sqs.send_message(
        QueueUrl=queue_url,
        MessageAttributes={
            'Location': {
                'DataType': 'String',
                'StringValue': data['Location']
            },
            'Cuisine': {
                'DataType': 'String',
                'StringValue': data['Cuisine']
            },
            'NumberOfPeople': {
                'DataType': 'String',
                'StringValue': data['NumberOfPeople']
            },
            'Date': {
                'DataType': 'String',
                'StringValue': data['Date']
            },
            'Time': {
                'DataType': 'String',
                'StringValue': data['Time']
            },
            'Email': {
                'DataType': 'String',
                'StringValue': data['Email']
            }
        },
        MessageBody='user restaurant request'
    )

    logger.debug(response)


""" --- Intents --- """


def dispatch(intent_request):
    """
    handle each intent
    """
    intent_name = intent_request['currentIntent']['name']

    if intent_name == 'GreetingIntent':
        return greeting(intent_request)
    elif intent_name == 'ThankYouIntent':
        return thank_you(intent_request)
    elif intent_name == 'DiningSuggestionsIntent':
        return dining_suggestions(intent_request)

    raise Exception('Intent with name ' + intent_name + ' not supported')


""" --- Main handler --- """


def lambda_handler(event, context):
    """
    main handler of events
    """
    # set the default time zone
    os.environ['TZ'] = 'America/New_York'
    time.tzset()

    logger.debug('event.bot.name={}'.format(event['bot']['name']))

    return dispatch(event)
