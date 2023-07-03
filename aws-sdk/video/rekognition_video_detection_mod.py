# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Purpose

Shows how to use the AWS SDK for Python (Boto3) with Amazon Rekognition to
recognize people and objects in videos.
"""

import logging
import json
from pprint import pprint
import time
import boto3
from botocore.exceptions import ClientError
import requests
import os

from rekognition_objects import (
    RekognitionFace, RekognitionCelebrity, RekognitionLabel, RekognitionText,
    RekognitionModerationLabel, RekognitionPerson)

logger = logging.getLogger(__name__)


class RekognitionVideo:
    """
    Encapsulates an Amazon Rekognition video. This class is a thin wrapper around
    parts of the Boto3 Amazon Rekognition API.
    """
    def __init__(self, video, video_name, rekognition_client):
        """
        Initializes the video object.

        :param video: Amazon S3 bucket and object key data where the video is located.
        :param video_name: The name of the video.
        :param rekognition_client: A Boto3 Rekognition client.
        """
        self.video = video
        self.video_name = video_name
        self.rekognition_client = rekognition_client
        self.topic = None
        self.queue = None
        self.role = None

    @classmethod
    def from_bucket(cls, s3_object, rekognition_client):
        """
        Creates a RekognitionVideo object from an Amazon S3 object.

        :param s3_object: An Amazon S3 object that contains the video. The video
                          is not retrieved until needed for a later call.
        :param rekognition_client: A Boto3 Rekognition client.
        :return: The RekognitionVideo object, initialized with Amazon S3 object data.
        """
        video = {'S3Object': {'Bucket': s3_object.bucket_name, 'Name': s3_object.key}}
        return cls(video, s3_object.key, rekognition_client)

    def does_role_exist(self, client, resource_name):
        roles = []
        response = client.list_roles()
        roles.extend(response['Roles'])
        while 'Marker' in response.keys():
            response = client.list_roles(Marker = response['Marker'])
            roles.extend(response['Roles'])

        role_flag = False
        print('roles found: ' + str(len(roles)))  
        for role in roles:
            if (role['RoleName'] == resource_name):
                print(role['RoleName'])
                role_flag = True

        return role_flag

    def does_policy_exist(self, client, resource_name):
        policies = []
        response = client.list_policies()
        policies.extend(response['Policies'])
        while 'Marker' in response.keys():
            response = client.list_policies(Marker = response['Marker'])
            policies.extend(response['Policies'])

        policy_flag = False
        print('policies found: ' + str(len(policies)))  
        for policy in policies:
            if (policy['PolicyName'] == resource_name):
                print(policy['PolicyName'])
                policy_flag = True
        
        return policy_flag
        
 
    def create_notification_channel(
            self, resource_name, iam_client, iam_resource, sns_resource, sqs_resource):
        """
        Creates a notification channel used by Amazon Rekognition to notify subscribers
        that a detection job has completed. The notification channel consists of an
        Amazon SNS topic and an Amazon SQS queue that is subscribed to the topic.

        After a job is started, the queue is polled for a job completion message.
        Amazon Rekognition publishes a message to the topic when a job completes,
        which triggers Amazon SNS to send a message to the subscribing queue.

        As part of creating the notification channel, an AWS Identity and Access
        Management (IAM) role and policy are also created. This role allows Amazon
        Rekognition to publish to the topic.

        :param resource_name: The name to give to the channel resources that are
                              created.
        :param iam_resource: A Boto3 IAM resource.
        :param sns_resource: A Boto3 SNS resource.
        :param sqs_resource: A Boto3 SQS resource.
        """
        self.topic = sns_resource.create_topic(Name=resource_name)
        self.queue = sqs_resource.create_queue(
            QueueName=resource_name, Attributes={'ReceiveMessageWaitTimeSeconds': '5'})
        queue_arn = self.queue.attributes['QueueArn']

        # This policy lets the queue receive messages from the topic.
        self.queue.set_attributes(Attributes={'Policy': json.dumps({
            'Version': '2008-10-17',
            'Statement': [{
                'Sid': 'test-sid',
                'Effect': 'Allow',
                'Principal': {'AWS': '*'},
                'Action': 'SQS:SendMessage',
                'Resource': queue_arn,
                'Condition': {'ArnEquals': {'aws:SourceArn': self.topic.arn}}}]})})
        self.topic.subscribe(Protocol='sqs', Endpoint=queue_arn)

        
        if (self.does_role_exist(iam_client, resource_name) == False):
            # This role lets Amazon Rekognition publish to the topic. Its Amazon Resource
            # Name (ARN) is sent each time a job is started.
            self.role = iam_resource.create_role(
                RoleName=resource_name,
                AssumeRolePolicyDocument=json.dumps({
                    'Version': '2012-10-17',
                    'Statement': [
                        {
                            'Effect': 'Allow',
                            'Principal': {'Service': 'rekognition.amazonaws.com'},
                            'Action': 'sts:AssumeRole'
                        }
                    ]
                })
            )
        else:
            self.role = iam_resource.Role(resource_name)
        
        if (self.does_policy_exist(iam_client, resource_name) == False):
            print("Policy does not exist.")
            policy = iam_resource.create_policy(
                PolicyName=resource_name,
                PolicyDocument=json.dumps({
                    'Version': '2012-10-17',
                    'Statement': [
                        {
                            'Effect': 'Allow',
                            'Action': 'SNS:Publish',
                            'Resource': self.topic.arn
                        }
                    ]
                })
            )
        else:
            policy = iam_resource.Policy(resource_name)
        
        self.role.attach_policy(PolicyArn=policy.arn)

    def get_notification_channel(self):
        """
        Gets the role and topic ARNs that define the notification channel.

        :return: The notification channel data.
        """
        return {'RoleArn': self.role.arn, 'SNSTopicArn': self.topic.arn}

    def delete_notification_channel(self):
        """
        Deletes all of the resources created for the notification channel.
        """
        for policy in self.role.attached_policies.all():
            self.role.detach_policy(PolicyArn=policy.arn)
            policy.delete()
        self.role.delete()
        logger.info("Deleted role %s.", self.role.role_name)
        self.role = None
        self.queue.delete()
        logger.info("Deleted queue %s.", self.queue.url)
        self.queue = None
        self.topic.delete()
        logger.info("Deleted topic %s.", self.topic.arn)
        self.topic = None

    def poll_notification(self, job_id):
        """
        Polls the notification queue for messages that indicate a job has completed.

        :param job_id: The ID of the job to wait for.
        :return: The completion status of the job.
        """
        status = None
        job_done = False
        while not job_done:
            messages = self.queue.receive_messages(
                MaxNumberOfMessages=1, WaitTimeSeconds=5)
            logger.info("Polled queue for messages, got %s.", len(messages))
            if messages:
                body = json.loads(messages[0].body)
                message = json.loads(body['Message'])
                if job_id != message['JobId']:
                    raise RuntimeError
                status = message['Status']
                logger.info("Got message %s with status %s.", message['JobId'], status)
                messages[0].delete()
                job_done = True
        return status

    def _start_rekognition_job(self, job_description, start_job_func):
        """
        Starts a job by calling the specified job function.

        :param job_description: A description to log about the job.
        :param start_job_func: The specific Boto3 Rekognition start job function to
                               call, such as start_label_detection.
        :return: The ID of the job.
        """
        try:
            response = start_job_func(
                Video=self.video, NotificationChannel=self.get_notification_channel())
            job_id = response['JobId']
            logger.info(
                "Started %s job %s on %s.", job_description, job_id, self.video_name)
        except ClientError:
            logger.exception(
                "Couldn't start %s job on %s.", job_description, self.video_name)
            raise
        else:
            return job_id

    def _get_rekognition_job_results(self, job_id, get_results_func, result_extractor):
        """
        Gets the results of a completed job by calling the specified results function.
        Results are extracted into objects by using the specified extractor function.

        :param job_id: The ID of the job.
        :param get_results_func: The specific Boto3 Rekognition get job results
                                 function to call, such as get_label_detection.
        :param result_extractor: A function that takes the results of the job
                                 and wraps the result data in object form.
        :return: The list of result objects.
        """
        try:
            response = get_results_func(JobId=job_id)
            logger.info("Job %s has status: %s.", job_id, response['JobStatus'])
            results = result_extractor(response)
            logger.info("Found %s items in %s.", len(results), self.video_name)
        except ClientError:
            logger.exception("Couldn't get items for %s.", job_id)
            raise
        else:
            return results

    def _do_rekognition_job(
            self, job_description, start_job_func, get_results_func, result_extractor):
        """
        Starts a job, waits for completion, and gets the results.

        :param job_description: The description of the job.
        :param start_job_func: The Boto3 start job function to call.
        :param get_results_func: The Boto3 get job results function to call.
        :param result_extractor: A function that can extract the results into objects.
        :return: The list of result objects.
        """
        job_id = self._start_rekognition_job(job_description, start_job_func)
        status = self.poll_notification(job_id)
        if status == 'SUCCEEDED':
            results = self._get_rekognition_job_results(
                job_id, get_results_func, result_extractor)
        else:
            results = []
        return results
    
    #Marcel    
    def do_text_detection(self):
        """
        Performs text detection on the video.

        :return: The list of texts found in the video.
        """
        return self._do_rekognition_job(
            "text detection",
            self.rekognition_client.start_text_detection,
            self.rekognition_client.get_text_detection,
            lambda response: [
                RekognitionText(text['TextDetection'], text['Timestamp']) 
                for text in response['TextDetections']])

    def do_label_detection(self):
        """
        Performs label detection on the video.

        :return: The list of labels found in the video.
        """
        return self._do_rekognition_job(
            "label detection",
            self.rekognition_client.start_label_detection,
            self.rekognition_client.get_label_detection,
            lambda response: [
                RekognitionLabel(label['Label'], label['Timestamp']) for label in
                response['Labels']])

    def do_face_detection(self):
        """
        Performs face detection on the video.

        :return: The list of faces found in the video.
        """
        return self._do_rekognition_job(
            "face detection",
            self.rekognition_client.start_face_detection,
            self.rekognition_client.get_face_detection,
            lambda response: [
                RekognitionFace(face['Face'], face['Timestamp']) for face in
                response['Faces']])

    def do_person_tracking(self):
        """
        Performs person tracking in the video. Person tracking assigns IDs to each
        person detected in the video and each detection event is associated with
        one of the IDs.

        :return: The list of person tracking events found in the video.
        """
        return self._do_rekognition_job(
            "person tracking",
            self.rekognition_client.start_person_tracking,
            self.rekognition_client.get_person_tracking,
            lambda response: [
                RekognitionPerson(person['Person'], person['Timestamp']) for person in
                response['Persons']])

    def do_celebrity_recognition(self):
        """
        Performs celebrity detection on the video.

        :return: The list of celebrity detection events found in the video.
        """
        return self._do_rekognition_job(
            "celebrity recognition",
            self.rekognition_client.start_celebrity_recognition,
            self.rekognition_client.get_celebrity_recognition,
            lambda response: [
                RekognitionCelebrity(celeb['Celebrity'], celeb['Timestamp'])
                for celeb in response['Celebrities']])

    def do_content_moderation(self):
        """
        Performs content moderation on the video.

        :return: The list of moderation labels found in the video.
        """
        return self._do_rekognition_job(
            "content moderation",
            self.rekognition_client.start_content_moderation,
            self.rekognition_client.get_content_moderation,
            lambda response: [
                RekognitionModerationLabel(label['ModerationLabel'], label['Timestamp'])
                for label in response['ModerationLabels']])


def usage_demo():
    print('-'*88)
    print("Welcome to the Amazon Rekognition video detection demo!")
    print('-'*88)

    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

    iam_client = boto3.client('iam')
    rekognition_client = boto3.client('rekognition')
    iam_resource = boto3.resource('iam')
    sns_resource = boto3.resource('sns')
    sqs_resource = boto3.resource('sqs')

    '''
    # Create new bucket and files on web 
    print("Creating Amazon S3 bucket and uploading video.")
    s3_resource = boto3.resource('s3')
    bucket = s3_resource.create_bucket(
        Bucket=f'doc-example-bucket-rekognition-{time.time_ns()}',
        CreateBucketConfiguration={
            'LocationConstraint': s3_resource.meta.client.meta.region_name
        })
    
    
    video_object = bucket.Object('bezos_vogel.mp4')
    bezos_vogel_video = requests.get(
        'https://dhei5unw3vrsx.cloudfront.net/videos/bezos_vogel.mp4', stream=True)
    video_object.upload_fileobj(bezos_vogel_video.raw)
    '''
    
    # Use pre-defines bucket and files from folder
    bucket_name = 'marcel-experiments'
    bucket_prefix_videos='rov/'
    bucket_prefix_json='json/'
    video_file_name='rov/rov_video_trim.mp4'
    print("Uploading the vide to the bucket {}".format(bucket_name))
    s3_resource = boto3.resource('s3') # type: botostubs.S3
    bucket = s3_resource.Bucket(bucket_name)

    #Upload all files in folder rov to s3
    for root, dirs, files in os.walk(bucket_prefix_videos):
        for filename in files:
            print("Moving {} to s3//:{}".format((bucket_prefix_videos+filename),bucket_name))
            video_object = bucket.Object(bucket_prefix_videos+filename)
            video_object.upload_file(bucket_prefix_videos+filename)
            video = RekognitionVideo.from_bucket(video_object, rekognition_client)
            
            f_filename = os.path.splitext(filename)[0]
            topic_name='doc-example-video-rekognition'+f_filename
            print("Creating notification channel {} from Amazon Rekognition to Amazon SQS.".format(topic_name))
            video.create_notification_channel(topic_name, iam_client, iam_resource, sns_resource, sqs_resource)
            
            print("Detecting texts in the video {}.".format(filename))
            labels = video.do_text_detection()

            #Save dictionary in file
            #file_json=bucket_prefix_json+f_filename+'.json'
            file_json=f_filename+'.json'
            with open(bucket_prefix_json+file_json, 'w') as fp:
                for label in labels:
                    json.dump(label.to_dict(), fp,  indent=4)
            
            print(f"Detected {len(labels)} texts, here are the first twenty:")
            for label in labels[:20]:
                pprint(label.to_dict_compact())
            input("Press Enter when you're ready to continue.")

            print("Deleting resources created for the demo.")
            video.delete_notification_channel()
            bucket.objects.delete()
            #bucket.delete()
            #logger.info("Deleted bucket %s.", bucket.name)
            print("All resources cleaned up. Thanks for watching!")
            print('-'*88)


if __name__ == '__main__':
    usage_demo()
