from typing import List, Dict, Optional
import json
import boto3
from botocore.exceptions import ClientError
from collections import defaultdict
import io
from moto import mock_aws

# QUEUE_URL = "https://sqs.eu-north-1.amazonaws.com/050451393944/MyQueue"
LAMBDA_FUNCTION_NAME = "ProcessNewDocument"

# AWS Clients
class DocumentMatcher:
    def __init__(self):
        self.samples: Dict[str, Dict] = {}
        self.documents: list[Dict] = []
        self.matches: Dict[str, List[Dict]] = {}
        
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        lambda_client = boto3.client('lambda', region_name='us-east-1')
        self.sqs = boto3.client('sqs', region_name='us-east-1')
        
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
    
    # def enqueue_document(self, document: Dict):
    #     self.sqs.send_message(QueueUrl=QUEUE_URL, MessageBody=json.dumps(document))
        
    #     try:
    #         response = self.invoke_lambda_for_processing(document)
    #         print(f"Lambda function invoked successfully: {response}")
    #     except Exception as e:
    #         print(f"Error invoking lambda function: {str(e)}")
            
    
    # def invoke_lambda_for_processing(self, document: Dict):
    #     """Invoke AWS Lambda to process the document."""
    #     response = lambda_client.invoke(
    #         FunctionName=LAMBDA_FUNCTION_NAME,
    #         InvocationType="Event",
    #         Payload=json.dumps(document)
    #     )
    #     return response
        
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
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
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

@mock_aws
def toy_document_matcher():
    print("\nTest 1\n")
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    
    # Creating a table
    sample_table = dynamodb.create_table(
        TableName='SampleTable',
        KeySchema=[{'AttributeName': 'sample_id', 'KeyType': 'HASH'}],
        AttributeDefinitions=[{'AttributeName': 'sample_id', 'AttributeType': 'S'}],
        ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
    )
    matched_table = dynamodb.create_table(
        TableName='MatchedDocuments',
        KeySchema=[{'AttributeName': 'sample_id', 'KeyType': 'HASH'}],
        AttributeDefinitions=[{'AttributeName': 'sample_id', 'AttributeType': 'S'}],
        ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
    )
    
    # Waiting for the table to be created
    sample_table.wait_until_exists()
    matched_table.wait_until_exists()
    
    matcher = DocumentMatcher()
    
    matcher.add_sample("Sample 1", {
        "Invoice Number": "poiu", 
        "Payment Amount": "$111", 
        "Company Name": "Alice's Apples", 
        "Date": "1-11-2023"
        })
        
    matcher.add_sample("Sample 2", {
        "Invoice Number": "abc", 
        "Payment Amount": "$321", 
        "Company Name": "Bob's Banana", 
        "Date": "1-11-2023"
        })

    matcher.add_sample("Sample 3", {
        "Invoice Number": "xkcd", 
        "Payment Amount": "$553", 
        "Company Name": "Charlie's Cherris", 
        "Date": "1-14-2023"
        })
    
    doc1 = {"key1": 1234, "key2": "abc", "key3": "qwer"} 
    doc2 = {"key1": "asdf", "key2": 1234, "key3": "xyz"} 

    matcher.add_document(doc1)
    matcher.add_document(doc2)
    
    print("\nPrinting Samples")
    print(json.dumps(matcher.get_samples(), indent=2))
    
    print("\nPrinting Matches")
    print(json.dumps(matcher.get_matches(), indent=2))
 
# toy_document_matcher()

@mock_aws
def test_document_matcher():
    print("\nTest 2\n")
    create_table("SampleTable")
    create_table("MatchedDocuments")
    
    matcher = DocumentMatcher()
    
    # Adding samples
    with open('samples.json', 'r') as file:
        sample_file = json.load(file)

        for sample_id, sample_desc in sample_file.items():
            matcher.add_sample(sample_id, sample_desc)
    
    # Adding documents
    with open('documents.json', 'r') as file:
        doc_file = json.load(file)
    
        for idx, doc in enumerate(doc_file.values()):
            matcher.add_document(doc)
        
    # Printing samples
    print("\nPrinting Samples")
    print(json.dumps(matcher.get_samples(), indent=2))
    
    # Printing documents
    print("\nPrinting Matches")
    print(json.dumps(matcher.get_matches(), indent=2))

test_document_matcher()
