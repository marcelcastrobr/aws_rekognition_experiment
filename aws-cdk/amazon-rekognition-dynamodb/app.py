#!/usr/bin/env python3
import os

import aws_cdk as cdk

from amazon_rekognition_dynamodb.amazon_rekogntion_dynamodb_stack import AmazonRekognitionDynamodbStack



# Example comes from https://github.com/aws-samples/amazon-rekognition-large-scale-processing

app = cdk.App()
AmazonRekognitionDynamodbStack(app, "AmazonRekogntionDynamodbStack",
    env=cdk.Environment(account='012006820026', region='us-east-1'),

    # For more information, see https://docs.aws.amazon.com/cdk/latest/guide/environments.html
    )

app.synth()
