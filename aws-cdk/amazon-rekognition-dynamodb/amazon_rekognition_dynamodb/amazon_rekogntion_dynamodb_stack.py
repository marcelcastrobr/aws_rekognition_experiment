import aws_cdk as cdk

from aws_cdk import (
    Duration,
    Stack,
    aws_sqs as sqs,
    aws_s3 as s3,
    aws_lambda as _lambda,
    aws_lambda_event_sources as _lambda_events,
    aws_s3_notifications as s3n,
    aws_iam as iam,
    aws_dynamodb as dynamodb,
    aws_sns as sns,
    aws_sns_subscriptions as subscriptions,
)
from constructs import Construct

class AmazonRekognitionDynamodbStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create image bucket
        image_bucket = s3.Bucket(self, 'inbound_image_s3_bucket')

        # Create the image processing queue
        image_process_queue = sqs.Queue(
            self, "image_process_queue",
            visibility_timeout=Duration.seconds(300),
            retention_period=Duration.days(1)
        )

        # Create the image response queue
        response_queue = sqs.Queue(
            self, "results_queue",
            visibility_timeout=Duration.seconds(300),
            retention_period=Duration.days(1)
        )

        # Create the sns topic and subscribe the queue to it
        topic = sns.Topic(self, "Topic", display_name="topic_detect_texts")
        topic.add_subscription(subscriptions.SqsSubscription(response_queue))
        
        

        my_queue_arn = response_queue.queue_arn
        my_topic_arn=topic.topic_arn
        response_queue.add_to_resource_policy(iam.PolicyStatement(
          actions=['sqs:SendMessage'],
          resources=[my_queue_arn],
          principals=[iam.ServicePrincipal("rekognition.amazonaws.com")],
          effect=iam.Effect.ALLOW,
          sid='test-sid'
        ))


        # Create role and let rekognition assume it in order to publish to the specific SNS topic.
        my_role = iam.Role(self, "My_Rekognition_Service_Role",
            assumed_by=iam.ServicePrincipal('lambda.amazonaws.com'),
            description="This is a custom role for rekognition to accces SNS"
            )
        my_role.add_managed_policy(iam.ManagedPolicy.from_managed_policy_arn(
            self,
            'My_managed_policy',
            'arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole'))
        my_role.assume_role_policy.add_statements(iam.PolicyStatement(
            actions=['sts:AssumeRole'],
            principals=[iam.ServicePrincipal("rekognition.amazonaws.com")],
            effect=iam.Effect.ALLOW
        ))


        # Set the put object notification to the SQS Queue
        image_bucket.add_event_notification(event=s3.EventType.OBJECT_CREATED,
                                            dest=s3n.SqsDestination(image_process_queue))
        
        # Define the AWS Lambda to call Amazon Rekognition DetectFaces
        detect_faces_lambda = _lambda.Function(self, 'detect_texts',
                                               function_name='AmazonRekognitionDynamodbStack_detect_texts',
                                               runtime=_lambda.Runtime.PYTHON_3_7,
                                               handler='detect_text.lambda_handler',
                                               role=my_role,
                                               code=_lambda.Code.from_asset('./lambda'),
                                               timeout=Duration.seconds(300),                                               
                                               environment={
                                                   'SQS_RESPONSE_QUEUE': response_queue.queue_name,
                                                   'SNS_TOPIC_ARN': topic.topic_arn,
                                                   'FUNCTION_NAME': 'AmazonRekognitionDynamodbStack_detect_texts',
                                                   },
                                               reserved_concurrent_executions=50
                                               )
        
        # Set SQS image_process_queue Queue as event source for detect_faces_lambda
        detect_faces_lambda.add_event_source(_lambda_events.SqsEventSource(image_process_queue,
                                                                           batch_size=1))

        # Allow response queue messages from lambda
        response_queue.grant_send_messages(detect_faces_lambda)

        # Allow lambda to call Rekognition by adding a IAM Policy Statement
        detect_faces_lambda.add_to_role_policy(iam.PolicyStatement(actions=['rekognition:*'],
                                                                   resources=['*']))
        
        # Allow to lambda:GetFunction in order to get the role_arn from within the function
        detect_faces_lambda.add_to_role_policy(iam.PolicyStatement(actions=['lambda:GetFunction','iam:PassRole'],
                                                                   resources=['*']))


        # Allow lambda to read from S3
        image_bucket.grant_read(detect_faces_lambda)


        # Define the DynamoDB Table
        results_table = dynamodb.Table(self, 'detect_text_results',
                                       table_name='detect_text_results',
                                       partition_key=dynamodb.Attribute(name='id', type=dynamodb.AttributeType.STRING),
                                       read_capacity=200,
                                       write_capacity=200
                                       )

        # Define the AWS Lambda to write results into DyanamoDB results_table
        write_results_lambda = _lambda.Function(self, 'write_results_text',
                                               runtime=_lambda.Runtime.PYTHON_3_7,
                                               function_name='AmazonRekognitionDynamodbStack_write_results_text',
                                               handler='write_results_text.lambda_handler',
                                               code=_lambda.Code.from_asset('./lambda'),
                                               timeout=Duration.seconds(30),
                                               environment={'TABLE_NAME': results_table.table_name}
                                               )

        # Set SQS response_queue Queue as event source for write_results_lambda results_table
        write_results_lambda.add_event_source(_lambda_events.SqsEventSource(response_queue,
                                                                            batch_size=1))

        # Allow AWS Lambda write_results_lambda to Write to Dynamodb
        results_table.grant_write_data(write_results_lambda)

        # Allow AWS Lambda write_results_lambda to read messages from the SQS response_queue Queue
        response_queue.grant_consume_messages(write_results_lambda)

        # Output to Amazon S3 Image Bucket
        cdk.CfnOutput(self, 'cdk_output',
                       value=image_bucket.bucket_name,
                       description='Input Amazon S3 Image Bucket')








