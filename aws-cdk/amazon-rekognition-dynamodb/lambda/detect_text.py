"""
Author: Marcel Cavalcanti de Castro
June 15th 2021
"""

import json
import logging
import boto3
import os
import re
from botocore.exceptions import ClientError
from time import sleep
from random import randint

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Environ Variables
SQS_RESPONSE_QUEUE = os.environ['SQS_RESPONSE_QUEUE']
SNS_TOPIC_ARN = os.environ['SNS_TOPIC_ARN']
REKOGNITION_CONFIDENCE = os.environ['REKOGNITION_CONFIDENCE']


MAX_RETRIES = 3

# Boto 3 Resources / Clients
logger.info("BOTO_VERSION: {}".format(boto3.__version__))
lambda_client = boto3.client('lambda')
rekognition_client = boto3.client('rekognition', region_name='us-east-1')



def detect_texts_rekognition(s3_bucket_name, s3_object_key, my_function_name):
    """
    Detect Rekognition text 
    :param s3_bucket_name: str that contains the bucket name
    :param s3_object_key: str that contains the object key name
    :return: Video Response dict / Error dict
    """
    retry_rekgonition = True
    num_retries = 0

    while retry_rekgonition and num_retries <= MAX_RETRIES:

        try:
            logger.info("Detecting {}/{}".format(s3_bucket_name, s3_object_key))
            logger.info("SQS_RESPONSE_QUEUE: {}".format(SQS_RESPONSE_QUEUE))
            logger.info("SNS_TOPIC_ARN: {}".format(SNS_TOPIC_ARN))
            logger.info("AWS_LAMBDA_FUNCTION_NAME: {}".format(my_function_name))

            response = lambda_client.get_function(FunctionName=my_function_name)
            lambda_config= response['Configuration']
            lambda_config_role = lambda_config['Role']
            logger.info("LAMBDA_ROLE: {}".format(lambda_config_role))

            #Make sure the job_tag string satisfy regular expression pattern expected
            # Ref. https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
            job_tag = re.sub('[^a-zA-Z0-9_.\\-:]+', '', str(s3_object_key))
            logger.info("JobTag: {}".format(job_tag))

            
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
                ,JobTag=job_tag
                ,Filters={
                    'WordFilter': {
                        'MinConfidence': float(REKOGNITION_CONFIDENCE)
                    }
                }
            )

            logger.info("Called start_text_detection for {}, got job_id: {}".format(job_tag, job_id))
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


def lambda_handler(event, context):
    """
    This function is call on a SQS Queue event, takes the message of the queue, parses the Amazon S3 PutObject event
    message, then calls Amazon Rekognition DetectFaces API, finally sends a message to the Write Results SQS Queue
    :param event: S3 PutObject Event dict
    :param context: Context dict
    :return: Amazon Rekognition DetectFaces Response Dict
    """

    logger.info("event: {}".format(event))

    logger.info("event['Records'][0]['body'][0]: {}".format(event['Records'][0]['body'][0]))
    message_body = json.loads(event['Records'][0]['body'])

    logger.info("message_body: {}".format(message_body))

    s3_bucket_name = message_body['Records'][0]['s3']['bucket']['name']
    s3_object_key = message_body['Records'][0]['s3']['object']['key']

    my_function_name = context.function_name
    logger.info("Bucket = {}".format(s3_bucket_name))
    logger.info("Object Key = {}".format(s3_object_key))

    return detect_texts_rekognition(s3_bucket_name, s3_object_key, my_function_name)
