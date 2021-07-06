## Purpose:

Show how to use AWS SDK for Python and AWS CDK with Amazon Rekognition to recognise text in video images.

- Detect text in a set of videos
- Create SNS notification tight to SQS queue to determine when a text detection job on a video has competed
- Record texts extracted from the video in a dynamoDB table



## Prerequisites:

- Python 3.7 or later
- Boto3 1.17.42 or later



## Running the code:

- For AWS SDK implementation see aws-sdk folder
- For AWS CDK implementation see aws-cdk folder