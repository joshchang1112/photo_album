import json
import boto3
import requests
import os
import json
from requests_aws4auth import AWS4Auth
from opensearchpy import OpenSearch, RequestsHttpConnection

region = 'us-east-1'
client = boto3.client('lex-runtime', region_name=region)
def lambda_handler(event, context):
    print(event)
    response = client.post_text(
        botName='extract_keyword_bot',
        botAlias='keyword_bot',
        userId='10',
        sessionAttributes={
            },
        requestAttributes={
            
        },
        inputText = event['queryStringParameters']['q']
    )
    keywords = []
    if 'slots' in response:
        for _, val in response['slots'].items():
            if val:
                keywords.append(val)
    
    
    service = 'es'
    credentials = boto3.Session().get_credentials()
    host = 'search-photos-ccm7gijqclbx5no7uitvl7iif4.us-east-1.es.amazonaws.com'
    index = 'photos'
    awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

    search = OpenSearch(
        hosts=[{'host': host, 'port': 443}],
        http_auth=awsauth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection
    )
    
    query = {
          'size': 10,
          'query': {
                'multi_match': {
                  'query': ' '.join(keywords),
                  'fields': ['labels']
                }
        }
    }

    response = search.search(
        body = query,
        index = index
    )
    image = []
    url_set = set()
    for hit in response['hits']['hits']:
        url = os.path.join(hit['_source']['bucket']+'.s3.amazonaws.com', hit['_source']['objectKey'])
        if url not in url_set:
            url_set.add(url)
            image.append(
                {
                    'url': url,
                    'labels': hit['_source']['labels']
                }
            )
    

    return {
        'statusCode': 200,
        "headers": {"Access-Control-Allow-Origin":"*","Content-Type":"application/json"},
        "body": json.dumps(image)
    }



