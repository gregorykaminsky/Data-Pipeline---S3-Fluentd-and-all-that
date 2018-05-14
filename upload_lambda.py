import os
import boto3
import time

'''
    This file creates/updates an existing Lambda function
'''


'''
    This file condtains the conda environment with all the associated packages zipped up
    Only conda installation appear to work with the 'avro' module
    You can create your own virtual environment by following instructions at:
        https://uoa-eresearch.github.io/eresearch-cookbook/recipe/2014/11/20/conda/
    when creating the environment, instead of:
        conda create -n yourenvname python=x.x anaconda
    use:
        conda create -n yourenvname python=x.x

    Conda install all the packages you need. (NEVER USE PIP TO INSTALL AVRO)
    Then navigate to site-packages folder in the ##yourenvname## directory lib/python3.6/site-packages
    and zip everything up:
        zip -r #name_of_file.zip# .
    This is the same zip file.

'''
start = time.time()
filename_zip = "Gregory_conda_env.zip"
os.system("rm " + filename_zip)
os.system("cp conda_env/" + filename_zip + " " + filename_zip)



'''
    all the python files are combined into a zip file
'''
list_files = os.listdir(".")
for file in list_files:
    if file.endswith(".py") == True:
        os.system("zip -ur " + filename_zip + " " + file)
        print(file)



client = boto3.client(
        's3',
        aws_access_key_id=os.environ['aws_access_key_id'],
        aws_secret_access_key=os.environ['aws_secret_access_key'],
    )
bucket = 'fivestars-kprod-braze-events'
link2 = 'currents/dataexport.prod-03.S3.integration.5a9ee171a12f74534a9a4e70/'


response = client.delete_object(
    Bucket=bucket,
    Key=link2 + filename_zip,
)


client.upload_file(Filename = filename_zip, Bucket=bucket, Key = link2 + filename_zip)

lambda_client = boto3.client('lambda')
functionName = "GregoryKaminsky_Braze_to_Treasure"

'''
    A lambda function is updated with the new zip file
    If this lambda function doesn't yet exist, create it with the method below
'''
response = lambda_client.update_function_code(
    FunctionName=functionName,
    Publish=True,
    S3Bucket=bucket,
    S3Key=link2 + filename_zip,
)


response = client.delete_object(
    Bucket='fivestars-kprod-braze-events',
    Key='currents/dataexport.prod-03.S3.integration.5a9ee171a12f74534a9a4e70/' + filename_zip,
)

'''
    This method creates and uploads a lambda function with a name given by the user.
    Handler variables is the name of the 'lambda_function.py' with a single method 'lambda_handler'
    This is the method that will be called every time a lambda function is executed.
    A master method.
'''

end = time.time()

print(end - start)
def create_function(functionName):

    #previous lambda function of the same name is deleted.
    try:
        response = lambda_client.delete_function(
        FunctionName=functionName
        )
    except Exception as e:
        print("That function doesn't exist = " + functionName)

    result = lambda_client.create_function(
        FunctionName=functionName,
        Runtime='python3.6',
        Role='arn:aws:iam::649240952810:role/service-role/MoveBrazeData',
        Handler='lambda_function.lambda_handler',
        Code={
            'S3Bucket': bucket,
            'S3Key': link2 + filename_zip,
            },
            Description='MoveData',
            Timeout=300,
            MemorySize=128,
            Publish=True,
            Environment={
            'Variables': {
                'aws_access_key_id':os.environ['aws_access_key_id'],
                'aws_secret_access_key':os.environ['aws_secret_access_key'],
                'td_apikey':os.environ['td_apikey'],
                }
            },
    )
