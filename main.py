import json
import boto3
import os
from typing import Dict, List
from botocore.exceptions import ClientError
from collections import defaultdict
import logging

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS clients
dynamodb = boto3.resource('dynamodb', region_name=os.getenv('AWS_DEFAULT_REGION'))
sqs = boto3.client('sqs', region_name=os.getenv('AWS_DEFAULT_REGION'))

class DocumentMatcher:
    def __init__(self):
        self.samples: Dict[str, Dict] = {}
        self.documents: List[Dict] = []
        self.matches: Dict[str, List[Dict]] = defaultdict(list)
        
        # Initializing DynamoDB tables
        sample_table_name = os.getenv("SAMPLE_TABLE_NAME", "SampleTable")
        matched_table_name = os.getenv("MATCHED_TABLE_NAME", "MatchedDocuments")
        
        self.sample_table = dynamodb.Table(sample_table_name)
        self.matched_table = dynamodb.Table(matched_table_name)
        
        # Loading existing samples from DynamoDB
        self.load_samples()

    def load_samples(self):
        """Loading samples from DynamoDB table on initialization"""
        try:
            logger.info("Loading samples from DynamoDB table...")
            response = self.sample_table.scan()
            for item in response.get('Items', []):
                self.samples[item['sample_id']] = item['description']
            logger.info(f"Loaded {len(self.samples)} samples successfully.")
        except ClientError as e:
            logger.error(f"Error loading samples: {e}", exc_info=True)
            raise
    
    def add_sample(self, sample_id: str, sample_description: Dict):
        """Add a new sample description"""
        self.samples[sample_id] = sample_description
        self.sample_table.put_item(Item={"sample_id": sample_id, "description": sample_description})
        
    def add_document(self, document: Dict):
        """Process a document and store the matches in DynamoDB"""
        logger.info(f"Processing document")
        try:
            self.documents.append(document)
            flattened_doc = self.flatten_doc(document)
            
            # Processing direct match
            matched_sample_ids = self.direct_match(flattened_doc)
            if matched_sample_ids:
                for sample_id in matched_sample_ids:
                    self.store_match(sample_id, document)
            
            # Processing indirect matches
            self.indirect_match(flattened_doc)
            logger.info(f"Proccessed document: {document}")
            
        except ClientError as e:
            logger.error(f"Error proccessing document: {e}", exc_info=True)
            raise

    def direct_match(self, document: Dict):
        """Finds direct matches with existing samples"""
        return [
            sample_id for sample_id, sample_desc in self.samples.items() 
            if any(doc_value in sample_desc.values() for doc_value in document.values())
            ]
            
    def indirect_match(self, new_doc: Dict):
        """Find indirect matches with other documents"""
        for doc in self.documents:
            for match_id, match_docs in self.matches.items():
                if any(self.has_matching_fields(doc, match_doc)
                       for match_doc in match_docs if doc != match_doc): 
                    self.store_match(match_id, doc)
    
    def has_matching_fields(self, doc1: Dict, doc2: Dict):
        """Check if the document contains any matching fields"""
        return any(doc1[key1] == doc2[key2] for key1 in doc1 for key2 in doc2)
        
    def store_match(self, sample_id: str, document: Dict):
        """Stores the matched documents in DynamoDB"""
        if document not in self.matches[sample_id]:
            self.matches[sample_id].append(document)
            try:
                # Update the existing list of documents
                self.matched_table.update_item(
                    Key={"sample_id": sample_id},
                    UpdateExpression="SET description = list_append(description, :doc)",
                    ExpressionAttributeValues={":doc": [document]},
                    ConditionExpression="attribute_exists(description)",
                )      
                logger.info(f"Match updated: {document}")
                          
            except ClientError as e:
                if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                    try:
                        self.matched_table.put_item(Item={"sample_id": sample_id, "description": [document]})
                        logger.info(f"Match inserted: {document}")
                        
                    except ClientError as e:
                        logger.error(f"Error inserting new match: {e}", exc_info=True)
                        raise
                else:
                    logger.error(f"Error updating matches: {e}", exc_info=True)
                    raise
    
    def enqueue_document(self, document: Dict):
        try:
            queue_url = os.getenv("QUEUE_URL")
            if not queue_url:
                raise ValueError("QUEUE_URL environment variable is not set")
            
            sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(document))
        except ClientError as e:
            print(f"Error enqueuing document: {e}")
            raise
    
    def flatten_doc(self, doc: Dict, parent_key: str = '', sep: str = '.') -> Dict:
        """Flatten nested document dictionary structure"""
        flattened = {}
        for k, v in doc.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                flattened.update(self.flatten_doc(v, new_key, sep))
            else:
                flattened[new_key] = v
        return flattened

# Initialize DocumentMatcher outside handler for Lambda reuse
matcher = DocumentMatcher()

def lambda_handler(event, context):
    """Process SQS messages and match documents"""
    try:
        for record in event['Records']:
            document = json.loads(record['body'])
            matcher.add_document(document)
            
        return {'statusCode': 200, 'body': document}
    
    except Exception as e:
        print(f"Error processing batch: {e}")
        return {'statusCode': 500, 'body': f'Error processing documents: {str(e)}'}
    