import time
import Modification_Gregory_event_driven
import lambda_function
from recursive_read_aws import import_from_aws
import TreasureData_to_Braze_lambda

start = time.time()
event = {}
record1 = {'test_number':'this is a test on my computer',  's3':{'object':{'key': 'currents/dataexport.prod-03.S3.integration.5a9ee171a12f74534a9a4e70/event_type%3Dusers.messages.email.Send/date%3D2018-04-26-20/315/prod-03/dataexport.prod-03.S3.integration.5a9ee171a12f74534a9a4e70%2B0%2B0000000071.avro' }}}
record2 = {'test_number':'this is a test on my computer',  's3':{'object':{'key': 'currents/dataexport.prod-03.S3.integration.5a9ee171a12f74534a9a4e70/event_type%3Dusers.messages.email.Send/date%3D2018-05-11-17/315/prod-03/dataexport.prod-03.S3.integration.5a9ee171a12f74534a9a4e70%2B0%2B0000001290.avro'}}}
record3 = {'test_number':'this is a test on my computer',  's3':{'object':{'key': 'currents/dataexport.prod-03.S3.integration.5a9ee171a12f74534a9a4e70/event_type%3Dusers.messages.email.Send/date%3D2018-05-11-17/315/prod-03/dataexport.prod-03.S3.integration.5a9ee171a12f74534a9a4e70%2B0%2B0000001290.avro' }}}





event['Records'] = [record1, record2, record3]

#Modification_Gregory_event_driven.lambda_handler(event, "test string")
TreasureData_to_Braze_lambda.lambda_handler(event, "test string")
#import_from_aws(clean_start = True)
#lambda_function.lambda_handler(0, 0)
end = time.time()
print(end - start)
