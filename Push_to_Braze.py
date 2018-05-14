import json
import requests
import os
from pprint import pprint
import inspect
import tdclient
import sys
import numpy as np



def main():

    '''
    for row in job.result():
        print(row)
    '''

    #link = 'https://harvest.greenhouse.io/v1/candidates'
    auth = "a58f29e4-44d7-4307-b0d5-c28a503a2adb"

    auth_stage = "40b5d04f-b837-4957-8e3d-9efe4c3b034b"
    #auth = "5263b05f-4adf-474d-a53e-0ba7c0975d2a"

    #this needs to work
    current_link = "/users/track"

    #this works
    current_link = "/segments/list"
    instance_url = []
    instance_url.append("https://rest.iad-01.braze.com")
    instance_url.append("https://rest.iad-02.braze.com")
    instance_url.append("https://rest.iad-03.braze.com")
    instance_url.append("https://rest.iad-04.braze.com")



    full_access_key = '40b5d04f-b837-4957-8e3d-9efe4c3b034b'
    link = instance_url[2] + current_link + "?api_key=" + auth_stage
    r = requests.get(link)
    payload = json.loads(r.text)

    pprint(payload)
    #pprint(payload)

    td =  tdclient.Client(os.environ['td_apikey'])

    the_query = "Select * from tmp_welcome_email_list"

    job = td.query(db_name = "braze", q = the_query, type = "presto")
    job.wait()

    output_array = []
    true_array = []
    schema = job.result_schema
    json_dump = []
    for row in job.result():
        output_array.append({})
        true_array.append(repr(row))
        for i in range(len(row)):
            output_array[-1][schema[i][0]] = row[i]

    attributes_array = []
    for row in output_array:
        temp_dic = {}
        temp_dic['external_id'] = row['external_id']
        temp_dic['email'] = row['email']
        temp_dic['first_name'] = row['first_name']
        temp_dic['phone'] = row['phone']
        attributes_array.append(temp_dic)

    dictionary = {}
    temp = np.asarray(output_array)
    dictionary['attributes'] = attributes_array


    dictionary['api_key'] = auth_stage

    r = requests.post(url = link, data = payload)

    current_link = "/users/track"
    link = instance_url[2] + current_link + "?api_key=" + auth_stage
    #link = instance_url[2] + current_link

    r = requests.post(url = link, data = dictionary)
    print('\n')
    print(r)
    print(r.reason)
    print(r.text)
    print(r.json)
    print(r.raw)
    print(r.request)
    print(r._content)
    print(r.url)
    print(r.status_code)
    print(r.headers)
    print(r.iter_lines)
    print('\n')

    #print(dir(r))
    link = instance_url[2] + current_link + "?api_key=" + '40b5d04f-b837-4957-8e3d-9efe4c3b034b'
    r = requests.get(url = link)
    print(r.text)
    print(r.request)
    print(r.json)

if __name__ == '__main__':
    main()
