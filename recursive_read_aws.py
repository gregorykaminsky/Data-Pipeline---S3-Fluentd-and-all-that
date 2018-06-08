import os
import json
import boto3
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import json
import tdclient

#bucket is located in        US East (N. Virginia)


'''
    Schema from an avro file is converted to schema used in presto database.
'''
def convert_schema_to_Presto(input_dict):
    name = input_dict['name']
    type = input_dict['type']
    comment = "\'" + input_dict['doc'] + "\'"

    if(isinstance(type, list) == True):
        type = 'VARCHAR'
    elif(type == 'string'):
        type = 'VARCHAR'
    elif(type == 'int' or type == 'long'):
        type = 'BIGINT'
    elif(type == 'float' or type == 'double'):
        type = 'DOUBLE'
    else:
        type = 'VARCHAR'
    return name + " " + type + " COMMENT " + comment

'''
    Given an address of a file, its schema is recorded and if such schema doesn't exist,
    a new table is created in braze database
'''
def new_schema_create_new_table(filename, table_name, database_name = "braze"):
    reader = DataFileReader(open(filename, "rb"), DatumReader())
    schema = json.loads(reader.meta['avro.schema'])
    create_table = "CREATE TABLE IF NOT EXISTS " + table_name
    all_field_string = ''
    for field in  schema['fields']:
        comma = ', '
        if(all_field_string == ""):
            comma = ' '
        all_field_string = all_field_string + comma + convert_schema_to_Presto(field)
    create_table = create_table + ' ( ' + all_field_string +  ' ); '
    td = tdclient.Client(os.environ['td_apikey'])
    job = td.query(database_name, create_table, type = "presto")
    job.wait()


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
    If there is a need to start from scratch, this method resets the all the dates to 2010 in the config file
    The entire transfer begins again
    WARNING: it can take a lot longer then the lifetime of a lambda function to run through all the files
'''
'''
    If there is a need to start from scratch, this method resets the all the dates to 2010 in the config file
    The entire transfer begins again
    WARNING: it can take a lot longer then the lifetime of a lambda function to run through all the files
'''

def clean_Reload(email_links, input_tables, database_name = "braze"):
    for key in email_links.keys():
        email_links[key]['date'] = 'date=2010-04-17-20/'
        email_links[key]['time'] = '00:00:00'
    td = tdclient.Client(os.environ['td_apikey'])

    for key in email_links:
        if(input_tables == [] or (key in input_tables)):
            table = key.split("/")[0]
            table = table.split(".")
            table_name = table[0] + "_" + table[1] + "_" + table[2] + "_" + table[3]


            drop_table = "DROP TABLE IF EXISTS " + table_name
            job = td.query(database_name, drop_table, type = "presto")
            job.wait()

'''
    This method pulls data from s3 and puts it to Treasure data, braze database
    The created temporary directory has to be placed in '/tmp/'
    Lambda doesn't allow for files to be written anywhere else
    @clean_start = Tries to transfer all the files in s3
    @input_tables = can specify which tables to transfer and which to leave alone.
                    format of the dictionary:

                    {'users.messages.email.Click/':'date=2010-04-17-20/', 'users.messages.email.Delivery/':'2010:00:00'}
'''
def import_from_aws(directory_avro = "/tmp/directory_avro",  clean_start = False, input_tables = {}, test = True):
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

    email_links = json.load(open("config.json", "rb")) #loaded as a dictionary
    if(clean_start == True and test == False):
        clean_Reload(email_links, input_tables)
    else:
        for table in input_tables:
            email_links[table]['date'] = input_tables[table]

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
        if(event not in input_tables):
            continue

        '''
            This is the last transferred date and time for this event event
            Necessary information to avoid duplicates.
        '''
        last_transferred_date, last_transferred_time = email_links[event]['date'], email_links[event]['time']

        #lists all files in a folder with the given prefix.
        result = client.list_objects(Bucket=bucket, Prefix=link2 + link3 + event, Delimiter='/').get('CommonPrefixes')

        if(len(result) > 999):
            print ("Severe problem, result is longer then 999")


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
                try:
                    reader = DataFileReader(open(filename, "rb"), DatumReader())
                except Exception as e:
                    pass #this needs to be expended
                '''
                '''
                for user in reader:
                    if user not in temp_json_output:
                        temp_json_output.append(user)

            for item in temp_json_output:
                json_output.append(item)

        if(clean_start == True):
            table_name = event.split("/")[0].split(".")
            table_name = table_name[0] + "_" + table_name[1] + "_" + table_name[2] + "_" + table_name[3]
            new_schema_create_new_table(filename = filename, table_name = table_name, database_name = "braze")


        #files are moved to a single file /tmp/temp.json
        json_file_name = "/tmp/temp.json"
        file_to_treasure = open("/tmp/temp.json", "w")
        for user in json_output:
            file_to_treasure.write(json.dumps(user) + '\n')
        file_to_treasure.close()

        #this single file is uploaded to Treasure Data.
        td =  tdclient.Client(os.environ['td_apikey'])
        try:
            if(test == True):
                print('This is a test on my computer')
                print('table_name:' + email_links[event]['table_name'])
                print()
            else:
                result = td.import_file(db_name = "braze", table_name = email_links[event]['table_name'], format = "json", file = json_file_name)
        except Exception as e:
            print(e)
    return "success"


'''
    All the avro files are read, converted to JSON then appended to a single list
    client = aws Client
    file_names = names of the avro files
    bucket = s3 bucket
    error_keys = if there are errors, this is the dictionary where error files would be stored
'''
def read_then_to_json(client, file_names, bucket, error_keys_table):
    temp_json_output = []


    for file in file_names:
        filename = "/tmp/temp.avro"
        try:
            client.download_file(Bucket = bucket, Key = file, Filename = filename)
        except Exception as e:
            ''' files which could not be downloaded'''
            print ("File could not be downloaded: " + file)
            error_keys_table['aws']['files'].append(file)
            continue

        try:
            reader = DataFileReader(open(filename , "rb"), DatumReader())

        except Exception as e:
            ''' files that couldn't be opened '''
            print ("File could not be opened: " + file)
            error_keys_table['open']['files'].append(file)
            continue

        for user in reader:
            if user not in temp_json_output:
                temp_json_output.append(user)
    return temp_json_output
