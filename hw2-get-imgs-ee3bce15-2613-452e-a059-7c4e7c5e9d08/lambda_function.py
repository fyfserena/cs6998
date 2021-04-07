# -*- coding: utf-8 -*-
"""
Created on Thu Mar 18 20:06:25 2021

@author: fyf
"""

import boto3
import json
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

def search(keys):
    service = 'es'
    region = 'us-west-2'
    # headers = {"Content-Type" : "application/json"}
    credentials = boto3.Session().get_credentials()
    awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)
        

    es = Elasticsearch(
        hosts = [{'host': 'search-photos-ofe6jnxvlnksqy5bihubm67r7u.us-west-2.es.amazonaws.com', 'port': 443}],
        http_auth = awsauth,
        use_ssl = True,
        verify_certs = True,
        connection_class = RequestsHttpConnection
    )
    
    results = []
    
    for key in keys:
        r = es.search(index="photos", body={"from":0,"size":10,"query":{"match":{"labels":key}}})
        for r in r["hits"]["hits"]:
            objKey = r['_source']['objectKey']
            # bucket = r["'_source'"]['bucket']
            results.append("https://cs6998-hw2-imgs-clf.s3-us-west-2.amazonaws.com/" + objKey)

    return results
    
    
def lambda_handler(event, context):
    query = event['queryStringParameters'].get("q")
    urls = []
    if not query: 
        urls = []
    else:
        qLst = query.lower().split(' ')
        andIdx = sum([i if qLst[i] == 'and' else 0 for i in range(len(qLst))])
        qLstClean = [qLst[andIdx-1], qLst[andIdx+1]] if andIdx else [qLst[-1]]
        qLstClean = [word[:-1] if word[-1] =='s' else word for word in qLstClean]
        urls = search(qLstClean)

    
    response = {
            'statusCode': 200,
            'headers': {
                'Access-Control-Allow-Headers': 'Content-Type',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'OPTIONS,POST,GET',
            },
            'body': json.dumps({ 
                "urls": urls
            }),
            'isBase64Encoded': False,
        }
                    
    return response



# comment
