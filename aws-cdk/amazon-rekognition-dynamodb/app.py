#!/usr/bin/env python3
import os

import aws_cdk as cdk

from amazon_rekognition_dynamodb.amazon_rekogntion_dynamodb_stack import AmazonRekognitionDynamodbStack, AmazonRekognitionDetectLabelDynamodbStack

ACCOUNT='754992829378'
REGION='eu-west-1'

# Example comes from https://github.com/aws-samples/amazon-rekognition-large-scale-processing

app = cdk.App()
#Production
AmazonRekognitionDynamodbStack(app, "rov-rekogntion-prod",
    env=cdk.Environment(account=ACCOUNT, region=REGION),
    )

#Development
AmazonRekognitionDynamodbStack(app, "rov-rekogntion-dev",
    env=cdk.Environment(account=ACCOUNT, region=REGION),
    # For more information, see https://docs.aws.amazon.com/cdk/latest/guide/environments.html
    )
# Use following to deploy dev: cdk deploy --app 'cdk.out/' AmazonRekogntionDynamodbStack-dev
#Development
AmazonRekognitionDetectLabelDynamodbStack(app, "detectlabels-rekogntion-dev",
    env=cdk.Environment(account=ACCOUNT, region=REGION),
    # For more information, see https://docs.aws.amazon.com/cdk/latest/guide/environments.html
    )




app.synth()
