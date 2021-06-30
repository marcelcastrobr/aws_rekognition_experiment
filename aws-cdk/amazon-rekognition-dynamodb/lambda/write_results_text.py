"""
Author: Marcel Cavalcanti de Castro
June 29th 2021
"""

import json
import boto3
from botocore.exceptions import ClientError
import os
import logging
from decimal import Decimal
from rekognition_objects import (RekognitionText)


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Environ Variables
TABLE_NAME = os.environ['TABLE_NAME']
SQS_RESPONSE_QUEUE = os.environ['SQS_RESPONSE_QUEUE']
# DynamoDB Resource
dynamodb_resource = boto3.resource('dynamodb', region_name='us-east-1')
sqs_resource = boto3.resource('sqs')
rekognition_client = boto3.client('rekognition', region_name='us-east-1')




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

def poll_notification(message_body):
    # Get the queue
    #queue = sqs_resource.get_queue_by_name(QueueName=SQS_RESPONSE_QUEUE)
    #messages = queue.receive_messages(
    #            MaxNumberOfMessages=1, WaitTimeSeconds=5)
    #logger.info("Polled queue for messages, got %s.", len(messages))
    #logger.info("Message: {}".format(messages))

    logger.info("message_body: {}".format(message_body))
    message = json.loads(message_body['Message'])
    job_id = message['JobId']
    status = message['Status']
    job_tag = message['JobTag']
    logger.info("job_tag: {}, job_id: {}, and status is {}".format(
        job_tag,
        job_id,
        status
    ))
    return job_tag, job_id, status

def _get_rekognition_job_results(job_id, get_results_func, result_extractor, next_token=None):
        """
        Gets the results of a completed job by calling the specified results function.
        Results are extracted into objects by using the specified extractor function.

        :param job_id: The ID of the job.
        :param next_token: Pagination token to retrieve the next set of text.
        :param get_results_func: The specific Boto3 Rekognition get job results
                                 function to call, such as get_text_detection.
        :param result_extractor: A function that takes the results of the job
                                 and wraps the result data in object form.
        :return: The list of result objects.
        """
        try:
            if next_token == None:
                response = get_results_func(JobId=job_id)
            else:
                response = get_results_func(JobId=job_id, NextToken=next_token)
            received_next_token = response.get('NextToken', None)
            logger.info("Job {} has status: {} and next token {}".format(
                job_id, 
                response['JobStatus'], 
                received_next_token
                ))
            results = result_extractor(response)
            logger.info("Found %s items in %s.", len(results), "test")
        except ClientError:
            logger.exception("Couldn't get items for %s.", job_id)
            raise
        else:
            return results, received_next_token

def parse_message_texts(message):
    """
    Parse the original Rekognition message to a DynamoDB compatible dict
    :param message: Rekognition Dict
    :return: Parsed dict to insert into DynamoDB Table
    """

    # Parse the Float values in the message to Decimals
    ddb_item = json.loads(json.dumps(message), parse_float=Decimal)

    return ddb_item


def lambda_handler(event, context):
    """
    This function is call on a SQS Queue event, takes the message of the queue, parses the message and inserts a item
    into dynamoDB
    :param event: SQS Message dict
    :param context: Context dict
    :return: DynamoDB Response PutItem dict
    """
    message_body = json.loads(event['Records'][0]['body'])
    logger.info("message_body: {}".format(message_body))
    job_tag, job_id, status = poll_notification(message_body)

    text_message = {}
    text_line_counter = 0
    token_number = 0
    next_token = None
    if status == 'SUCCEEDED':
        while (next_token != None)  or (token_number == 0):
            token_number=token_number+1
            logger.info("{} job_id: {}, status: {}, next_token: {}".format(token_number, job_id,status,next_token))
            texts, next_token = _get_rekognition_job_results(job_id,
                rekognition_client.get_text_detection, 
                lambda response: [
                    RekognitionText(text['TextDetection'], text['Timestamp']) 
                    for text in response['TextDetections']]
                ,next_token)        
            logger.info(f"Detected {len(texts)} texts, here are the first twenty:")
            for text in texts:
                #print(text.to_dict_compact())
                text_line_counter=text_line_counter+1
                line_message = text.to_dict_compact()
                text_message[text_line_counter] = line_message
            
        text_message.update({'id': job_tag })
        item = parse_message_texts(text_message)
        put_item_dynamodb(item)
    else:
        logger.info("Failure: job_id is: {}, and status is {}".format(job_id,status))

    #item = parse_message_emotions(message_body)
    #return put_item_dynamodb(item)

    return None

