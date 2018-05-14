import os
import json
import boto3
import fast_push_to_treasure
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import json
import tdclient

#bucket is located in        US East (N. Virginia)


'''
    Parse the date information from the <file_name_string>
    This is necessary so that all the information is not pulled at the same time.
'''

def get_aws_Dates(answer):
    all_dates = []
    for date in answer:
        date = date.strip()
        if(date != ''):
            date = date.split()[1]
            all_dates.append(date)
    return sorted(all_dates, reverse = True)




'''
    Parse the date information from the <file_name_string>
    This is necessary so that all the information is not pulled at the same time.
'''
def get_boto_Dates(answer):
    result = []
    for item in answer:
        result.append(item.get('Prefix').split('/')[3] + "/")
    return sorted(result, reverse = True)




'''
    Eliminates spaces in an array consisting of strings
'''
def eliminate_space(input):
    output = []
    for line in input:
        line = line.strip()
        if(line != ''):
            output.append(line)
    return output



'''
    If there is a need to start from scratch, this method resets the all the dates to 2010 in the config file
    The entire transfer begins again
    WARNING: it can take a lot longer then the lifetime of a lambda function to run through all the files
'''
def clean_config(email_links):
    for key in email_links.keys():
        email_links[key]['date'] = 'date=2010-04-17-20/'
        email_links[key]['time'] = '00:00:00'


'''
    This method pulls data from s3 and puts it to Treasure data, braze database
    The created temporary directory has to be placed in '/tmp/'
    Lambda doesn't allow for files to be written anywhere else
'''
def import_from_aws(directory_avro = "/tmp/directory_avro"):

    '''
        Location of the relevant files in the bucket
    '''
    link2 = 'currents/dataexport.prod-03.S3.integration.5a9ee171a12f74534a9a4e70/'
    link3 = 'event_type='

    #boto3 client environmental parameters are set.
    client = boto3.client(
        's3',
        aws_access_key_id=os.environ['aws_access_key_id'],
        aws_secret_access_key=os.environ['aws_secret_access_key'],
    )
    bucket = 'fivestars-kprod-braze-events'

    #The configuration file is downloaded from s3 bucket where it is stored.
    output = client.download_file(Bucket=bucket, Key=link2 + "config.json", Filename = "/tmp/config.json")
    email_links = json.load(open("/tmp/config.json", "rb")) #loaded as a dictionary



    '''
        Each event is a folder in the s3 bucket corresponded to the event type.
        events:
            users.messages.email.Bounce/
            users.messages.email.Click/
            users.messages.email.Delivery/
            ...
            That way one event type is processed at a time.
    '''
    for event in email_links.keys():
        '''
            This is the last transferred date and time for this event event
            Necessary information to avoid duplicates.
        '''
        last_transferred_date, last_transferred_time = email_links[event]['date'], email_links[event]['time']

        #lists all files in a folder with the given prefix.
        result = client.list_objects(Bucket=bucket, Prefix=link2 + link3 + event, Delimiter='/').get('CommonPrefixes')

        if(len(result) > 999):
            pass
            '''
            problem that has to be fixed.
            This implies that some results have been left behind. Amazon does not return lists larger then 1000
            This will be done in the next couple of days.
            '''


        '''
            The maximal dates that would be transfered are recorded

        '''
        all_dates = get_boto_Dates(result) #the date is actually part of the file name
        if(last_transferred_date <= all_dates[0]):
            equal_files = client.list_objects(Bucket=bucket, Prefix=link2 + link3 + event + all_dates[0]).get('Contents')
            all_time = []
            for file in equal_files:
                time = file['LastModified'].time()
                all_time.append(str(time.hour) + ":" + str(time.minute) + ":" + str(time.second))

            '''
                The term 'import_time' differes from 'time' because the files have not yet
                been uploaded to Treasure data, so this is treated as temporary time until final
                upload is made
            '''
            email_links[event]['import_time'] = max(all_time)
            email_links[event]['import_date'] = all_dates[0]
        else:
            continue



        '''
            The list of all files with dates greater then the last_transferred date is compiled.
        '''

        json_output = []
        for date in all_dates:
            if(last_transferred_date > date):
                break

            output = client.list_objects(Bucket=bucket, Prefix=link2 + link3 + event + date).get('Contents')
            files_to_download = []
            for filename in output:
                location = filename['Key']
                output_location = location.split("/")
                if(last_transferred_date == date):
                    time = filename['LastModified'].time()
                    time = str(time.hour) + ":" + str(time.minute) + ":" + str(time.second)
                    if(time > last_transferred_time):
                        files_to_download.append(location)
                else:
                    files_to_download.append(location)
            '''
                Up to this point no files were actually downloaded, but instead a list of files to download
                was compiled. The next step is below.
            '''

            '''
                The main work happens here,
                -files are downloaded,
                -stored briefly in /tmp/temp.avro
                -converted to JSON
                -combined to a single large array
                -checked for duplicates.
            '''
            temp_json_output = []
            for file in files_to_download:
                filename = "/tmp/temp.avro"
                client.download_file(Bucket = bucket, Key = file, Filename = filename)
                reader = DataFileReader(open(filename, "rb"), DatumReader())
                for user in reader:
                    if user not in temp_json_output:
                        temp_json_output.append(user)

            for item in temp_json_output:
                json_output.append(item)

        #files are moved to a single file /tmp/temp.json
        json_file_name = "/tmp/temp.json"
        file_to_treasure = open("/tmp/temp.json", "w")
        for user in json_output:
            file_to_treasure.write(json.dumps(user) + '\n')
        file_to_treasure.close()

        #this single file is uploaded to Treasure Data.
        td =  tdclient.Client(os.environ['td_apikey'])
        try:
            result = td.import_file(db_name = "braze", table_name = email_links[event]['table_name'], format = "json", file = json_file_name)

            #if the line above went well, the new updated time is saved to the config.json class.
            email_links[event]['time'] = email_links[event]['import_time']
            email_links[event]['date'] = email_links[event]['import_date']
        except Exception as e:
            print(e)
        #config.json is uploaded back to s3 bucket.
        client.put_object(Body = json.dumps(email_links, indent = 4, sort_keys = True), Bucket=bucket, Key=link2 + "config.json")
    return "success" #Lambda function returns here.
