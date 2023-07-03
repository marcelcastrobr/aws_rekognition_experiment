#Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#PDX-License-Identifier: MIT-0 (For details, see https://github.com/awsdocs/amazon-rekognition-developer-guide/blob/master/LICENSE-SAMPLECODE.)
# Ref: https://docs.aws.amazon.com/rekognition/latest/dg/labels-detect-labels-image.html

import boto3
from botocore.exceptions import ClientError
from io import BytesIO
import json
import logging
import os

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


# Environment variables
TABLE_NAME = os.environ['TABLE_NAME']
REKOGNITION_CONFIDENCE = os.environ['REKOGNITION_CONFIDENCE']

# DynamoDB Resource
dynamodb = boto3.resource('dynamodb')
rekognition_client = boto3.client('rekognition')


# A function called find_values is used to get just the labels from the response. 
# The name of the image and its labels are then ready to be uploaded to your DynamoDB table.
def find_values(id, json_repr):
    results = []

    def _decode_dict(a_dict):
        try:
            results.append(a_dict[id])
        except KeyError:
            pass
        return a_dict

    json.loads(json_repr, object_hook=_decode_dict) # Return value ignored.
    return results


#Load the images and labels into the DynamoDB table you created.
def load_data(image_labels, dynamodb=None):

    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')

    table = dynamodb.Table(TABLE_NAME)

    print("Adding image details:", image_labels)
    try:
        put_item_response = table.put_item(Item=image_labels)
        return put_item_response

    except ClientError as error:
        return error.response
    



def detect_labels(bucket_name, file_name):

    logger.info("Detecting {}/{}".format(bucket_name, file_name))

    # get the labels for the image by calling DetectLabels from Rekognition
    response = rekognition_client.detect_labels(
        Image={'S3Object': {'Bucket': bucket_name, 
                            'Name': file_name}},
                            MaxLabels=20
                            # Uncomment to use image properties and filtration settings
                            #Features=["GENERAL_LABELS", "IMAGE_PROPERTIES"],
                            #Settings={"GeneralLabels": {"LabelInclusionFilters":["Person"]},
                            # "ImageProperties": {"MaxDominantColors":10}}
                            ,MinConfidence=int(REKOGNITION_CONFIDENCE))

    print('Detected labels for ' + file_name)
    image_name = file_name

    for label in response['Labels']:
        for category in label['Categories']:
            labels_dict = {}
            labels_dict["Image"] = str(image_name)
            labels_dict["Label_Name"] = str(label['Name'])
            labels_dict["Label_Confidence"] = int(label['Confidence'])
            labels_dict["Label_Category"] = str(category['Name'])
            if len(label['Aliases']) >= 1:
                 for alias in label['Aliases']:
                    labels_dict["Label_Aliases"] = str(alias['Name'])
                    load_data(labels_dict)
            else:
                labels_dict["Label_Aliases"] = ''
                load_data(labels_dict)

    return len(response['Labels'])


def lambda_handler(event, context):

    logger.info("event: {}".format(event))
    logger.info("context: {}".format(context))
    

    logger.info("event['Records'][0]: {}".format(event['Records'][0]))
    message_body = event['Records'][0]

    logger.info("message_body: {}".format(message_body))

    s3_bucket_name = message_body['s3']['bucket']['name']
    s3_object_key = message_body['s3']['object']['key']

    logger.info("Bucket = {}".format(s3_bucket_name))
    logger.info("Object Key = {}".format(s3_object_key))

    return detect_labels(s3_bucket_name, s3_object_key)