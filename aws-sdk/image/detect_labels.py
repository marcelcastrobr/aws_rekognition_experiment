#Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#PDX-License-Identifier: MIT-0 (For details, see https://github.com/awsdocs/amazon-rekognition-developer-guide/blob/master/LICENSE-SAMPLECODE.)
# Ref: https://docs.aws.amazon.com/rekognition/latest/dg/labels-detect-labels-image.html

import boto3
from botocore.exceptions import ClientError
from io import BytesIO
import json

# Environment variables
TABLE_NAME = 'Images2'
REKOGNITION_CONFIDENCE='50'
MAX_LABELS=49



#  table named Images. The table has a composite primary key consisting of a partition key called Image and a sort key called Labels. 
# The Image key contains the name of the image, while the Labels key stores the labels assigned to that Image. 
def create_new_table(dynamodb=None):
    dynamodb = boto3.resource(
        'dynamodb',)
    # Table defination
    table = dynamodb.create_table(
        TableName=TABLE_NAME,
        KeySchema=[
            {
                'AttributeName': 'Image',
                'KeyType': 'HASH'  # Partition key
            },
            {
                'AttributeName': 'Label_Name',
                'KeyType': 'RANGE'  # Sort key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'Image',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'Label_Name',
                'AttributeType': 'S'
            },
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )
    return table


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

    # get the labels for the image by calling DetectLabels from Rekognition
    client = boto3.client('rekognition')
    response = client.detect_labels(
        Image={'S3Object': {'Bucket': bucket_name, 
                            'Name': file_name}},
                            MaxLabels=MAX_LABELS
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

                #print(labels_dict)
                #load_data(labels_dict)



    '''
    for label in response['Labels']:
        print("Label: " + label['Name'])
        print("Confidence: " + str(label['Confidence']))
        print("Instances:")

        for instance in label['Instances']:
            print(" Bounding box")
            print(" Top: " + str(instance['BoundingBox']['Top']))
            print(" Left: " + str(instance['BoundingBox']['Left']))
            print(" Width: " + str(instance['BoundingBox']['Width']))
            print(" Height: " + str(instance['BoundingBox']['Height']))
            print(" Confidence: " + str(instance['Confidence']))
            print()

        print("Parents:")
        for parent in label['Parents']:
            print(" " + parent['Name'])

        print("Aliases:")
        for alias in label['Aliases']:
            print(" " + alias['Name'])
            
            print("Categories:")
        for category in label['Categories']:
            print(" " + category['Name'])
            print("----------")
            print()

    if "ImageProperties" in str(response):
        print("Background:")
        print(response["ImageProperties"]["Background"])
        print()
        print("Foreground:")
        print(response["ImageProperties"]["Foreground"])
        print()
        print("Quality:")
        print(response["ImageProperties"]["Quality"])
        print()
        '''
    
    return len(response['Labels'])



def main():
    #device_table = create_new_table()
    #print("Status:", device_table.table_status)

    #photo = 'colorado-lake.jpeg'
    #photo = 'Screenshot 2023-06-21 at 1.14.08 PM.png'
    # get argument from command line
    import sys
    photo = sys.argv[1]
    #bucket = sys.argv[2]
    #bucket = 'rov-rekogntion-dev-inboundvideos3bucketb6f542e1-u2putp35h3or'
    bucket = 'detectlabels-rekogntion-inboundimagess3bucket406-1i19ijeeaczd0'
    label_count = detect_labels(bucket, photo)
    print("Labels detected: " + str(label_count))
    

    

if __name__ == "__main__":
    main()
