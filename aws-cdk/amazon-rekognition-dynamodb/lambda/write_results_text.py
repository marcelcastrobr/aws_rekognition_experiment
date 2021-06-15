"""
Author: Marcel Cavalcanti de Castro
June 15th 2021
"""

import json
import boto3
from botocore.exceptions import ClientError
import os
from decimal import Decimal

# Environ Variables
TABLE_NAME = os.environ['TABLE_NAME']
# DynamoDB Resource
dynamodb_resource = boto3.resource('dynamodb', region_name='us-east-1')


def put_item_dynamodb(item):
    """
    Call DynamoDB API PutItem
    :param item: Dictionary of id, FaceDetails and ResponseMetdata
    :return: DynamoDB PutItem response dict
    """
    dynamodb_table = dynamodb_resource.Table(TABLE_NAME)

    try:
        put_item_response = dynamodb_table.put_item(Item=item)
        return put_item_response

    except ClientError as error:
        return error.response


def parse_message_emotions(message):
    """
    Parse the original SQS message to a DynamoDB compatible dict
    :param message: SQS Message Dict
    :return: Parsed dict to insert into DynamoDB Table
    """
    text_details = message['TextDetections'][0]
    response_metadata = message['ResponseMetadata']
    # photo_id = str(uuid1()).split('-')[0]
    photo_id = message['s3_object_key']

    item = {'id': photo_id,
            'TextDetails': text_details,
            'ResponseMetadata': response_metadata}

    # for emotion in face_emotions:
    #    emotion_type = emotion['Type']
    #    emotion_type = emotion_type.lower()
    #    emotion_confidence = emotion['Confidence']
    #    item[emotion_type] = Decimal(str(emotion_confidence))
    #    print(f'{emotion_type} = {emotion_confidence}')

    # Parse the Float values in the message to Decimals
    ddb_item = json.loads(json.dumps(item), parse_float=Decimal)

    return ddb_item


def lambda_handler(event, context):
    """
    This function is call on a SQS Queue event, takes the message of the queue, parses the message and inserts a item
    into dynamoDB
    :param event: SQS Message dict
    :param context: Context dict
    :return: DynamoDB Response PutItem dict
    """
    print(event)
    print(event['Records'][0]['body'][0])
    message_body = json.loads(event['Records'][0]['body'])
    print(message_body)

    item = parse_message_emotions(message_body)
    return put_item_dynamodb(item)
