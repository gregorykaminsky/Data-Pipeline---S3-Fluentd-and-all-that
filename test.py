import time
import Modification_Gregory_event_driven
from recursive_read_aws import import_from_aws
import TreasureData_to_Braze_lambda

start = time.time()
event = {}
record1 = {'test_number':'this is a test on my computer',  's3':{'object':{'key': 'currents/dataexport.prod-03.S3.integration.5a9ee171a12f74534a9a4e70/event_type%3Dusers.messages.email.Send/date%3D2018-04-26-20/315/prod-03/dataexport.prod-03.S3.integration.5a9ee171a12f74534a9a4e70%2B0%2B0000000071.avro' }}}
record2 = {'test_number':'this is a test on my computer',  's3':{'object':{'key': 'currents/dataexport.prod-03.S3.integration.5a9ee171a12f74534a9a4e70/event_type%3Dusers.messages.email.Send/date%3D2018-05-11-17/315/prod-03/dataexport.prod-03.S3.integration.5a9ee171a12f74534a9a4e70%2B0%2B0000001290.avro'}}}
record3 = {'test_number':'this is a test on my computer',  's3':{'object':{'key': 'currents/dataexport.prod-03.S3.integration.5a9ee171a12f74534a9a4e70/event_type%3Dusers.messages.email.Send/date%3D2018-05-11-17/315/prod-03/dataexport.prod-03.S3.integration.5a9ee171a12f74534a9a4e70%2B0%2B0000001290.avro' }}}
record4 = {'test_number':'this is a test on my computer',  's3':{'object':{'key':'currents/dataexport.prod-03.S3.integration.5a9ee171a12f74534a9a4e70/event_type%3Dusers.messages.email.Click/date%3D2018-06-02-17/316/prod-03/dataexport.prod-03.S3.integration.5a9ee171a12f74534a9a4e70%2B0%2B0000072378.avro'}}}
#record5 = {'test_number':'this is a test on my computer',  's3':{'object':{'key':'currents/dataexport.prod-03.S3.integration.5a9ee171a12f74534a9a4e70/event_type=users.messages.email.Delivery/date=2018-06-07-18/304/prod-03/dataexport.prod-03.S3.integration.5a9ee171a12f74534a9a4e70+0+0000098736.avro' }}}
record5 = {'test_number':'this is a test on my computer',  's3':{'object':{'key':'currents/dataexport.prod-03.S3.integration.5a9ee171a12f74534a9a4e70/event_type%3Dusers.messages.email.Delivery/date%3D2018-06-07-18/304/prod-03/dataexport.prod-03.S3.integration.5a9ee171a12f74534a9a4e70%2B0%2B0000098849.avro' }}}


event['Records'] = [record1, record2, record3, record4, record5]

Modification_Gregory_event_driven.lambda_handler(event, "test_number")
input_tables = {'users.messages.email.Click/':'date=2010-04-17-20/', 'users.messages.email.Delivery/':'2010:00:00'}

#import_from_aws(clean_start = True, input_tables = input_tables, test = True)
end = time.time()
print(end - start)
