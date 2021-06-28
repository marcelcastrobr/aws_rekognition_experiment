"""
Author: Marcel Cavalcanti de Castro
June 15th 2021
"""

import json
import logging
import boto3
from botocore.exceptions import ClientError
import os
from time import sleep
from random import randint
# from rekognition_objects import (RekognitionText)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Environ Variables
SQS_RESPONSE_QUEUE = os.environ['SQS_RESPONSE_QUEUE']
SNS_TOPIC_ARN = os.environ['SNS_TOPIC_ARN']
AWS_LAMBDA_FUNCTION_NAME = os.environ['FUNCTION_NAME']


MAX_RETRIES = 3

# Boto 3 Resources / Clients
logger.info("BOTO_VERSION: {}".format(boto3.__version__))
iam_client = boto3.client('iam')
lambda_client = boto3.client('lambda')
rekognition_client = boto3.client('rekognition', region_name='us-east-1')
sqs_resource = boto3.resource('sqs')
iam_resource = boto3.resource('iam')
sns_resource = boto3.resource('sns')



def poll_notification(job_id):
    """
        Polls the notification queue for messages that indicate a job has completed.

        :param job_id: The ID of the job to wait for.
        :return: The completion status of the job.
        """
    # Get the queue
    queue = sqs_resource.get_queue_by_name(QueueName=SQS_RESPONSE_QUEUE)

    status = None
    job_done = False
    while not job_done:
        messages = queue.receive_messages(
            MaxNumberOfMessages=1,
            WaitTimeSeconds=5)
        logger.info("Polled queue for messages, got %s.", len(messages))
        if messages:
            body = json.loads(messages[0].body)
            message = json.loads(bod['Message'])
            if job_id != message['JobId']:
                raise RuntimeError
            status = message['Status']
            logger.info("Got message %s with status %s.",
                        message['JobId'], status)
            messages[0].delete()
            job_done = True
    return status


def detect_texts_rekognition(s3_bucket_name, s3_object_key, attributes='ALL'):
    """
    Detect Rekognition text 
    :param s3_bucket_name: str that contains the bucket name
    :param s3_object_key: str that contains the object key name
    :return: Video Response dict / Error dict
    """
    retry_rekgonition = True
    num_retries = 0

    resource_name = 'doc-video-rekognition-'+s3_object_key
    #create_notification_channel(resource_name)

    while retry_rekgonition and num_retries <= MAX_RETRIES:

        try:
            logger.info("Detecting {}/{}".format(s3_bucket_name, s3_object_key))
            logger.info("SQS_RESPONSE_QUEUE: {}".format(SQS_RESPONSE_QUEUE))
            logger.info("SNS_TOPIC_ARN: {}".format(SNS_TOPIC_ARN))
            logger.info("AWS_LAMBDA_FUNCTION_NAME: {}".format(AWS_LAMBDA_FUNCTION_NAME))
            response = lambda_client.get_function(FunctionName=AWS_LAMBDA_FUNCTION_NAME)
            lambda_config= response['Configuration']
            lambda_config_role = lambda_config['Role']
            logger.info("LAMBDA_ROLE: {}".format(lambda_config_role))

            # print(f'Detecting {s3_bucket_name}/{s3_object_key}')
            # print("SNS_TOPIC_ARN={}".format(topic.arn))

            # Calling start_text_detection
            job_id = rekognition_client.start_text_detection(
                Video={
                    'S3Object': {
                        'Bucket': s3_bucket_name,
                        'Name': s3_object_key
                    }
                }
                 ,NotificationChannel={
                    'SNSTopicArn': SNS_TOPIC_ARN,
                    'RoleArn': lambda_config_role
                    }
            )

            print(job_id)

            '''
            status = poll_notification(job_id)
            
            if status == 'SUCCEEDED':
                # results = self._get_rekognition_job_results(job_id, get_results_func, result_extractor)
                response = rekognition_client.get_text_detection(JobId=job_id)
                logger.info("Job %s has status: %s.", job_id, response['JobStatus'])
                retry_rekgonition = False
            else:
                results = []

            print(results)
            '''


            '''
            # Check if the call to Rekognition returned any value
            if image_response:
                # Add the object key in the message to send to Write Queue
                image_response['s3_object_key'] = s3_object_key

                # Send Mesage to SQS Queue
                send_response_sqs(image_response)
                retry_rekgonition = False

            return image_response
            '''
            return job_id

        except ClientError as error:

            if num_retries == MAX_RETRIES:
                raise error

            if error.__class__.__name__ == 'ThrottlingException' or\
                    error.__class__.__name__ == 'ProvisionedThroughputExceededException':

                num_retries += 1
                wait_time = 0.10 * (2 ** num_retries)
                rand_jitter = randint(200, 1000) / 1000
                sleep(wait_time + rand_jitter)

            elif error.__class__.__name__ == 'LimitExceededException':
                print(error.__class__.__name__)
                print(error.response)
                retry_rekgonition = False
                return error.response

            else:
                print(error.response)
                retry_rekgonition = False
                return error.response


def send_response_sqs(message_body):
    """
    Sends the response from Amazon Rekognition DetectFaces to a SQS queue
    :param message_body: Message to send to SQS queue dict
    :return:
    """
    # Get the queue
    queue = sqs_resource.get_queue_by_name(QueueName=SQS_RESPONSE_QUEUE)

    # Send a new message
    try:
        response = queue.send_message(MessageBody=json.dumps(message_body))
        message_id = response.get('MessageId')

        print(f'Sent Message {message_id}')

    except ClientError as error:
        print(error.response)
        return error.response


def lambda_handler(event, context):
    """
    This function is call on a SQS Queue event, takes the message of the queue, parses the Amazon S3 PutObject event
    message, then calls Amazon Rekognition DetectFaces API, finally sends a message to the Write Results SQS Queue
    :param event: S3 PutObject Event dict
    :param context: Context dict
    :return: Amazon Rekognition DetectFaces Response Dict
    """

    print("Inside lambda_handler")
    print("event:")
    print(event)

    print("event['Records'][0]['body'][0]:")
    print(event['Records'][0]['body'][0])
    message_body = json.loads(event['Records'][0]['body'])

    print("message_body:")
    print(message_body)

    s3_bucket_name = message_body['Records'][0]['s3']['bucket']['name']
    s3_object_key = message_body['Records'][0]['s3']['object']['key']

    print(f'Bucket = {s3_bucket_name}')
    print(f'Object Key = {s3_object_key}')

    return detect_texts_rekognition(s3_bucket_name, s3_object_key)
