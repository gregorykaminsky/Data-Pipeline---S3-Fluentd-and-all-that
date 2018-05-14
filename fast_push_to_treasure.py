import os
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import json
import tdclient
from inspect import getargspec



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
    No frills avro to JSON converter
    Cannot check schema or anything else
'''
def convert_to_JSON(input_files, destination_directory):
    for file in input_files:
        reader = DataFileReader(open(file, "rb"), DatumReader())
        output = open(destination_directory + indiv_file + ".json", "w")
        for user in reader:
            output.write(json.dumps(user) + '\n')
        output.close()
        reader.close()

'''
    Convert to json
'''
def file_convert_to_JSON(input_files):
    output = []
    for file in input_files:
        reader = DataFileReader(open(file, "rb"), DatumReader())
        for user in reader:
            if user not in output:
                output.append(user)
    return output


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
    Bulk import to import several files at a time to treasure data
'''

def bulk_import_to_td(link, td_client, table_name, database_name = "braze"):
    command = "find " + link + "  -type f -name" + " \"*.json\" "
    list_files = eliminate_space((os.popen(command).read()).split('\n'))
    if(list_files == []):
        return True

    session_name = "session-%d" % (int(time.time()),)
    bulk_import = td_client.create_bulk_import(session_name, database_name, table_name)
    try:
        for file_name in list_files:
            part_name = "part-%s" % (file_name.split(".")[-3], )
            bulk_import.upload_file(part_name, "json", file_name)
        bulk_import.freeze()
    except:
        bulk_import.delete()
        raise
        print("Import to Treasure data has failed, unknown exception")
        return False
    bulk_import.perform(wait=True)
    if 0 < bulk_import.error_records:
        warnings.warn("detected %d error records." % (bulk_import.error_records,))
        print("Import to Treasure data has failed, error in records")
        return False
    if 0 < bulk_import.valid_records:
        print("imported %d records." % (bulk_import.valid_records,))
    else:
        raise(RuntimeError("no records have been imported: %s" % (repr(bulk_import.name),)))
        return False
    result = bulk_import.commit(wait=True)
    print(result)
    return True



'''
    Large number of files are pushed to Treasure Data simoltaneously .
    Currently not used
'''
def push_to_treasure(directory_avro = "/tmp/directory_avro", directory_json = "/tmp/directory_json"):
    link1 = '/event_type=users.messages.'
    apikey = os.environ['td_apikey']
    td =  tdclient.Client(apikey)

    file = open("config.json", "rb")
    email_links = json.load(file)
    file.close()

    for event in email_links.keys():
        print("Pushing to Treasure data, event = " + event)
        link_json = directory_json + link1 + event
        table_name = event.split(".")[0] + "_" + event.split(".")[1]
        result = False
        result = bulk_import_to_td(link = link_json, td_client = td, database_name = "braze", table_name = table_name)
        if(result == True):
            print("Push to Treasure Data for event=" + event + " was successful. Files have been transferred.")
            os.system("rm -rf " + link_json + "*")
            email_links[event]['date'] = email_links[event]['import_date']
            email_links[event]['import_date'] = 'None'
            email_links[event]['time'] = email_links[event]['import_time']
            email_links[event]['import_time'] = 'None'

        file = open("config.json", "w")
        json.dump(email_links, file, indent = 4, sort_keys = True)
        file.close()

'''
    Imports a single file to Treasure Data
'''
def import_single_file(name, td_client):
    td =  tdclient.Client(os.environ['td_apikey'])
    td.import_file(db_name = "braze", table_name = table_name, format = "json", file = file_name)
