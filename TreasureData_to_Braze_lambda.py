import json
import requests
import os
import inspect
import tdclient

'''
    This is the lambda function to pull data from a table in Treasure Data,
    and push it to Braze staging
'''
def lambda_handler(event, context):
    td =  tdclient.Client(os.environ['td_apikey'])


    ''' Query is created and then sent'''
    the_query = "Select * from tmp_welcome_email_list"
    job = td.query(db_name = "braze", q = the_query, type = "presto")
    job.wait()

    ''' Result of the query is put into a list of JSON dictionaries'''
    output_array = []
    schema = job.result_schema
    for row in job.result():
        output_array.append({})
        for i in range(len(row)):
            output_array[-1][schema[i][0]] = row[i]

    ''' The JSON files are stored to output to Braze'''
    dictionary = {}
    dictionary['attributes'] = output_array
    dictionary['api_key'] = auth_stage

    ''' Location of the link to upload to braze '''
    instance_url = "https://rest.iad-03.braze.com"
    current_link = "/users/track"
    auth_stage = os.environ['auth_stage']
    link = instance_url + current_link + "?api_key=" + auth_stage

    '''
        This command doesn't work, I could not figure out why.
        It returns an error "500" which means server error on the part of Braze.
        Everything before it works.
    '''
    r = requests.post(url = link, data = json.dumps(dictionary))
    print('\n')
    print(r)
    print(r.text)
    print(r.json)
    print(r.raw)
