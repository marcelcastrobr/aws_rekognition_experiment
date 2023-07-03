# Amazon Rekognition:  text detection using AWS SDK for Python 



NOTE: Most part of the code was adapted from the [AWS Example on Rekognition]( https://github.com/awsdocs/aws-doc-sdk-examples/tree/master/python/example_code/rekognition). 

## Purpose:

Show how to use AWS SDK for Python with Amazon Rekognition to recognise text in video images.

- Detect text in a set of videos on a given S3 bucket.

- Create SNS notification tight to SQS queue to determine when a text detection job of a given video has been competed

- Record texts extracted from the video in a json file

  

## Architecture:

![image-20210602153523991](README.assets/image-20210602153523991.png)





## Running the code

There is one demonstration in this code:

* Detecting text from a video

For further examples on image detection and face collection see [AWS Example on Rekognition]( https://github.com/awsdocs/aws-doc-sdk-examples/tree/master/python/example_code/rekognition). 

* Detecting items in a single image.

* Building a collection of indexed faces and searching for matches.

* Detecting items in a video.

  

**Video detection**

Run this example at a command prompt with the following command.

```
python rekognition_video_detection_mod.py
```

**rekognition_video_detection_mod.py**

Shows how to use Amazon Rekognition video detection APIs. The `usage_demo` script 
starts detection jobs that detect texts in a video. 

Because video detection is performed asynchronously, the example also shows how to create 
a notification channel that uses Amazon Simple Notification Service (Amazon SNS) and
Amazon Simple Queue Service (Amazon SQS) to let the code poll for a job completion 
message.

## Additional information

- [Boto3 Amazon Rekognition service reference](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/rekognition.html)
- [Amazon Rekognition documentation](https://docs.aws.amazon.com/rekognition)

