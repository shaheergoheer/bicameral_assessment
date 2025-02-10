import boto3
import json
import os
from dotenv import load_dotenv

# Loading environment variables
load_dotenv()
sqs = boto3.client('sqs', region_name=os.getenv('AWS_DEFAULT_REGION'))
queue_url = os.getenv("QUEUE_URL")

with open("documents.json", "r") as file:
    document_file = json.load(file)
    
for idx, doc in enumerate(document_file.values()):
    response = sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(doc)
    )
    
    print("Response status code:", response['ResponseMetadata']['HTTPStatusCode'])

