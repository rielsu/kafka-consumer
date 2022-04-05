import json
import boto3   
import os 
from kafka import KafkaConsumer
from datetime import date, datetime

BUCKET_NAME = os.getenv('BUCKET_NAME')
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
KAFKA_BROKER = os.getenv('KAFKA_BROKER').split(',')

consumer = KafkaConsumer ('demo-kafka-topic',bootstrap_servers = KAFKA_BROKER)

s3 = boto3.resource('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
#s3 = boto3.resource('s3', aws_access_key_id='AKIAVK3WWRUFTXGKLZKM', aws_secret_access_key='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')
s3object = s3.Object( 
    BUCKET_NAME,
    '{today}/kafka_test_{now}.json'.format(
        today=str(datetime.today()),
        now=str(datetime.now())
    )
)

for message in consumer:
    print(message)
    s3object = s3.Object( 
        BUCKET_NAME,
        '{today}/kafka_test_{now}.json'.format(
            today=str(datetime.today()),
            now=str(datetime.now())
        )
    )
    s3object.put(
        #Body=(bytes(json.dumps(message[6]).encode('UTF-8')))
        Body=(bytes(json.dumps(message[6])))
    )

# json_data = {
# 'eventId': '7a378b69-673a-4fbd-935a-84c2af636f9d',
# 'eventDate': '2022-04-04 14:26:30.817654',
# 'businessProcess': 'PolicyEnrollment',
# 'eventClass': 'CaseEvent', 'eventType': 'CaseChange',
# 'eventStatus': 'Success', 'market': 'Broad', 'sourceSystem': 'Intake',
# 'sourceState': {'3rdParty': 'LexisNexis', '3rdPartyService': 'Risk Classifier'}
# }




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