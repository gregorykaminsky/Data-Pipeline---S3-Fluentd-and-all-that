

import boto3
import os
from recursive_read_aws import import_from_aws
'''
    This is the method that is called every time the lambda is activated.
'''
def lambda_handler(event, context):
    result = import_from_aws(directory_avro = "/tmp/directory_avro")
    return result
