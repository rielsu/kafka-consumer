import json
import boto3   
import os 
from kafka import KafkaConsumer
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

BUCKET_NAME = os.getenv('BUCKET_NAME')
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
KAFKA_BROKER = os.getenv('KAFKA_BROKER').split(',')

consumer = KafkaConsumer ('demo-kafka-topic',bootstrap_servers = KAFKA_BROKER)

s3 = boto3.resource('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
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
        Body=(bytes(json.dumps(message[6]).encode('UTF-8')))
    )
