import boto3
import os
import sys
import uuid
import json
import recursive_read_aws
import tdclient
from urllib.parse import unquote_plus


'''
    Get the name of the table from the key. Determine where a given file should be placed.
'''
def getTable(key):
    step1 = key.split("/")[2]
    step2 = step1.split("=")
    table = step2[1].replace('.', '_')
    return table

'''
    If you wish to transfer different messages, you need to add an
    entry to this array
'''
email_campaigns = ['users.messages.email.Bounce', 'users.messages.email.Click',
                   'users.messages.email.Delivery', 'users.messages.email.Open',
                   'users.messages.email.Send', 'users.messages.email.Unsubscribe']

'''
    This screens out the wrong keys that are not in the allowed
    email campaigns. If you want more options, modify 'email_campaigns' array above.
'''
def allowed_keys(key):
    for camp in email_campaigns:
        if key.find(camp) >= 0:
            return True
    return False


'''
    The errors are written to a log file
'''
def log_errors(error_keys, client, bucket, link2):
    if error_keys != {}:
        name_error_file = "master_error_file.json"
        client.download_file(Bucket = bucket, Key = link2 + name_error_file, Filename = "/tmp/" + name_error_file)
        error_data = json.load(open("/tmp/" + name_error_file, 'rb'))

        for table in error_data:
            for type in error_data[table]:
                if error_keys[table][type]['files'] != []:
                    for file in error_keys[key][type]['files']:
                        error_data[key][type]['files'].append(file)

        with open(name_error_file, 'w') as fp:
            json.dump(error_data, fp)
        client.upload_file(Filename = "/tmp/" + name_error_file, Bucket = bucket, Key = link2 + name_error_file)

'''
    Empty logs are initialized
'''
def empty_logs():
    return {'aws':{'doc':'files that could not be downloaded from aws', 'files':[]},
    'open':{'doc':'files that could not be opened', 'files':[]},
    'td':{'doc':'files that could not be uploaded to Treasure Data', 'files':[]},
    'time':{'doc':'files not transferred because lambda function was approaching the 5 min time limit', 'files':[]}}




'''
    The code for the Lambada function. This is the method that is called when Lambda is executed
'''
def lambda_handler(event, context):
    link2 = 'currents/dataexport.prod-03.S3.integration.5a9ee171a12f74534a9a4e70/'

    #boto3 client environmental parameters are set.
    client = boto3.client(
        's3',
        aws_access_key_id=os.environ['aws_access_key_id'],
        aws_secret_access_key=os.environ['aws_secret_access_key'],
    )

    bucket = 'fivestars-kprod-braze-events'

    all_keys = {}
    error_keys = {}
    for record in event['Records']:

        key = record['s3']['object']['key']
        '''
            Important Gotcha - unquote_plus removes strange extra characters that appear in 'key'
        '''
        key = unquote_plus(key)
        if allowed_keys(key) == False:
            continue

        table = getTable(key)
        if table not in all_keys:
            all_keys[table] = []
        all_keys[table].append(key)

    '''
        Create all the error logs for all the all_keys
    '''
    for table in all_keys:
        if table not in error_keys:
            error_keys[table] = empty_logs()

    '''
        Here all files are read and uploaded to Treasure Data
    '''

    td =  tdclient.Client(os.environ['td_apikey'])
    for table in all_keys:
        ''' Files for a given table are read and transfered to a dictionary'''
        json_output = recursive_read_aws.read_then_to_json(client = client, file_names = all_keys[table], bucket = bucket, error_keys_table = error_keys[table])
        json_file_name = "/tmp/temp.json"
        file_to_treasure = open("/tmp/temp.json", "w")
        for user in json_output:
            file_to_treasure.write(json.dumps(user) + '\n')
        file_to_treasure.close()
        if "test_number" in record:
            print(record['test_number'])
            print(table)
        else:
            if(json_output != []):
                try:
                    result = td.import_file(db_name = "braze", table_name = table, format = "json", file = json_file_name)
                except Exception as e:
                    print ('Transfer failed for filenames: ' + str(all_keys[table]))
                    '''
                        In the event of exception and a failed transfer, all the names of the failed avro
                        files are written to the error_keys and eventually to the logs.
                    '''
                    for file in all_keys[table]:
                        error_keys[table]['td']['files'].append(file)

    ''' Errors are written to a log file '''
    if(context != "test_number"):
        pass
    log_errors(error_keys = error_keys, client = client, bucket = bucket, link2 = link2)
    return 'success'
