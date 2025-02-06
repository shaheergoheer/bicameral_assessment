from typing import List, Dict, Optional
import json
import boto3
from botocore.exceptions import ClientError
from collections import defaultdict
import os
from moto import mock_aws
from dotenv import load_dotenv

# Loading environment variables
load_dotenv()

# AWS clients
dynamodb = boto3.resource('dynamodb', region_name=os.getenv('AWS_DEFAULT_REGION'))
s3 = boto3.client('s3', region_name=os.getenv('AWS_DEFAULT_REGION'))
sqs = boto3.client('sqs', region_name=os.getenv('AWS_DEFAULT_REGION'))
lambda_client = boto3.client('lambda', region_name=os.getenv('AWS_DEFAULT_REGION'))

QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/050451393944/MyQueue"
LAMBDA_FUNCTION_NAME = "ProcessNewDocument"

class DocumentMatcher:
    def __init__(self):
        self.samples: Dict[str, Dict] = {}
        self.documents: list[Dict] = []
        self.matches: Dict[str, List[Dict]] = {}
        
        self.sample_table = dynamodb.Table("SampleTable")
        self.matched_table = dynamodb.Table("MatchedDocuments")

    def add_sample(self, sample_id: str, sample_description: Dict):
        self.samples[sample_id] = sample_description
        self.sample_table.put_item(Item={"sample_id": sample_id, "description": sample_description})
    
    def add_document(self, document: Dict):
        """This function takes a document, matches it, and adds it to a table."""
        self.documents.append(document)
        flattened_doc = self.flatten_doc(document)
        
        matched_sample_ids = self.direct_match(flattened_doc)

        # If direct matches are found
        if matched_sample_ids:
            for sample_id in matched_sample_ids:
                if sample_id not in self.matches:
                    self.matches[sample_id] = []
                
                # Same document will not be added to matches
                if document not in self.matches[sample_id]:
                    self.matches[sample_id].append(document)
                    self.matched_table.put_item(Item={"sample_id": sample_id, "description": document})
                
        self.indirect_match(flattened_doc)
        
    def direct_match(self, document: Dict):
        """The function loops through each sample and compares the document with every sample, 
        returning a list of keys that identify the samples the document matched."""
        matched_samples = [] # List of keys for all matched samples
        for sample_id, sample_desc in self.samples.items():
            
            # Matching documents by their field values
            if any(doc_value in sample_desc.values() for doc_value in document.values()):
                matched_samples.append(sample_id)
        return matched_samples if matched_samples else None
    
    def indirect_match(self, new_doc: Dict):
        """ The function loops through all documents and compares each document with every document in the 
        matches dictionary. The document which indirectly matches to another document is added to the list."""
        for doc in self.documents:
            for match_id, match_docs in self.matches.items():
                for match_doc in match_docs:
                    if doc == match_doc: # Same document is not added
                        continue
                    
                    # Matching a document with any of its attributes
                    if any(doc[key1] == match_doc[key2] for key1 in doc for key2 in match_doc):
                        if doc not in self.matches[match_id]:   # Document should not be added if it already exists
                            self.matches[match_id].append(doc)
                            self.matched_table.put_item(Item={"sample_id": match_id, "description": doc})
    
    def enqueue_document(self, document: Dict):
        sqs.send_message(QueueUrl=QUEUE_URL, MessageBody=json.dumps(document))
    
    def get_samples(self):
        return self.samples

    def get_matches(self):
        return self.matches

    def flatten_doc(self, doc, parent_key='', sep='.'):
        """ 
        The function flattens a nested dictionary for easy traversal and comparison.
        """
        flattened_doc={}
        for key, value in doc.items():
            new_key = f"{parent_key}{sep}{key}" if parent_key else key
            if isinstance(value, dict):
                flattened_doc.update(self.flatten_doc(value, key, sep))
            else:
                flattened_doc[new_key] = value
        return flattened_doc

@mock_aws 
def create_table(table_name):
    """This function creates a table with the given table name."""
    try:
        table = dynamodb.Table(table_name)
        table.load()
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            table = dynamodb.create_table(
                TableName=table_name,
                KeySchema=[{'AttributeName': 'sample_id', 'KeyType': 'HASH'}],
                AttributeDefinitions=[{'AttributeName': 'sample_id', 'AttributeType': 'S'}],
                ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
            )
            table.wait_until_exists()
        else:
            raise e

# Initializing DocumentMatcher instance. 
matcher = DocumentMatcher()

# Lambda handler function for asynchronous data handling
def lambda_handler(event, context):
    for record in event['Records']:
        document = json.loads(record['body'])
        try:
            matcher.add_document(document)
            
        except Exception as e:
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'message': 'Error processing document',
                    'error': str(e)
                })
            }
    return {
        "statusCode": 200,
        'body': json.dumps({
            'message': 'Documents processed successfully'
        })
    }

def main():
    # Creating sample and match tables
    create_table("SampleTable")
    create_table("MatchedDocuments")

    # Loading samples
    with open("samples.json", "r") as file:
        samples_file = json.load(file)
        
    for sample_id, sample_desc in samples_file.items():
        matcher.add_sample(sample_id, sample_desc)
    
    print(json.dumps(matcher.get_samples(), indent=2))
        
    with open("documents.json", "r") as file:
        document_file = json.load(file)
        
    for doc in document_file.values():
        # Testing lambda_handler
        event = {"Records": [{"body": json.dumps(doc)}]}
        context = {}
        
        # matcher.enqueue_document(doc)
        lambda_response = lambda_handler(event, context)
        print(lambda_response)

    print(json.dumps(matcher.get_matches(), indent=2))
    
if __name__ == "__main__":
    main()
    
    