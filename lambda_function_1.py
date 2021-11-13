import json
import urllib.parse
import boto3
import requests
import random
from requests_aws4auth import AWS4Auth
from opensearchpy import OpenSearch, RequestsHttpConnection
from datetime import datetime
from decimal import Decimal

region = 'us-east-1'
def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    print(bucket, key)
    labels = []
    # Get Label from Rekognition
    if event['Records'][0]["eventName"] == "ObjectCreated:Put":
        rekognition = boto3.client('rekognition')
        response = rekognition.detect_labels(Image={'S3Object':{'Bucket':bucket,'Name':key}}, 
                                            MinConfidence=80)
        
        for res in response['Labels']:
            labels.append(res['Name'])
    
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket, Key=key)
    # Get Label from Metadata
    if 'customlabels' in response['Metadata']:
        customlabels_list = response['Metadata']['customlabels'].split(',')
        for c_label in customlabels_list:
            labels.append(c_label.strip())
    

    service = 'es'
    credentials = boto3.Session().get_credentials()
    host = 'search-photos-ccm7gijqclbx5no7uitvl7iif4.us-east-1.es.amazonaws.com'
    awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

    photo_body = {
        "objectKey": key,
        "bucket": bucket,
        "createdTimestamp": event['Records'][0]['eventTime'],
        "labels": labels
    }

    search = OpenSearch(
        hosts=[{'host': host, 'port': 443}],
        http_auth=awsauth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection
    )

    url = host + 'photos'
    while True:
        try:
            random_id = random.randint(1, 1000000)
            search.get(index="photos", doc_type="_doc", id=str(random_id))
        except:
            response = search.index(index='photos', doc_type="_doc", id=str(random_id), body=photo_body)
            break
    
    # search.get(index="photos", doc_type="_doc", id=str(123))
    return {'label': labels}


