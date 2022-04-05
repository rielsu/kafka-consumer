import json
import boto3   
import os 
from kafka import KafkaConsumer

BUCKET_NAME = os.getenv('BUCKET_NAME')
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
KAFKA_BROKER = os.getenv('KAFKA_BROKER').split(',')

consumer = KafkaConsumer ('demo-kafka-topic',bootstrap_servers = KAFKA_BROKER)

s3 = boto3.resource('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
s3object = s3.Object( BUCKET_NAME, 'now/test.json')

for message in consumer:
    print(message)
    print(message[6])

# json_data = {
# 'eventId': '7a378b69-673a-4fbd-935a-84c2af636f9d',
# 'eventDate': '2022-04-04 14:26:30.817654',
# 'businessProcess': 'PolicyEnrollment',
# 'eventClass': 'CaseEvent', 'eventType': 'CaseChange',
# 'eventStatus': 'Success', 'market': 'Broad', 'sourceSystem': 'Intake',
# 'sourceState': {'3rdParty': 'LexisNexis', '3rdPartyService': 'Risk Classifier'}
# }

# s3object.put(
#     Body=(bytes(json.dumps(json_data).encode('UTF-8')))
# )


'''
{
'eventId': '7a378b69-673a-4fbd-935a-84c2af636f9d',
'eventDate': '2022-04-04 14:26:30.817654',
'businessProcess': 'PolicyEnrollment',
'eventClass': 'CaseEvent', 'eventType': 'CaseChange',
'eventStatus': 'Success', 'market': 'Broad', 'sourceSystem': 'Intake',
'sourceState': {'3rdParty': 'LexisNexis', '3rdPartyService': 'Risk Classifier'}
}
'''