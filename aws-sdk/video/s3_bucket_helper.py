import botostubs, boto3
import logging
import json
from pprint import pprint
import time
import os
import botocore

# IN orde to have autocomplete on VScode for boto3
# https://mypy-boto3.readthedocs.io/en/latest/#installation

logger = logging.getLogger(__name__)

client = boto3.client('iam')
iam_resource = boto3.resource('iam')




resource_name='alation_kms_admin-policy'
#'doc-example-video-rekognitionrov_video_trim'

def does_policy_exist(client, resource_name):
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
            policy_flag = True
    
    return policy_flag
        


if (does_policy_exist(client, resource_name) == False):
        print("Policy {} does not exist!".format(resource_name))
else:
    policy = client.get_policy(PolicyArn=('arn:aws:iam::012006820026:policy/'+resource_name))
    print(policy.arn)
    print(iam_resource.Policy(resource_name).arn)


