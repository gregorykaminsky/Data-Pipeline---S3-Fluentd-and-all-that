## Manual
This is the manual to help understand the code.
# Modification_Gregory_event_driven.py
* Python module that has the code for the lambda function
* This lambda gets data from S3 and outputs it to Treasure Data.
* It is triggered every time a collection of files is uploaded to S3
* The errors are uploaded to an 'master_error_file.json' file in S3.

# upload_lambda.py
* Module that contains code to create or modify a given lambda function.
* Creates and uploads the conda environment for the python.

# test.py
* Module that can test different lambda functions.
* Clocks a single execution of a lambda functions.
* Can call recursive_read_aws.py for a full clean install of data in Treasure Data.    

# recursive_read_aws.py
* Can create a clean install of all the Treasure Data tables created by 'Modification...driven.py'
