# -*- coding: utf-8 -*-
"""
Created on Thu Mar 18 20:07:41 2021

@author: fyf
"""

import boto3
import requests
import datetime
import json
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth


client_s3 = boto3.client('s3')
client_rek = boto3.client('rekognition')

def getImgMetaData(input):
    response = client_s3.head_object(
        Bucket=input['bucket'],
        Key=input['key']
    )
    metadata = response['Metadata']
    custom_labels = []
    if metadata:
        # string variable containing tags
        custom_labels = metadata['customlabels'].lower().split(',')
        
    return custom_labels

def labelImg(input):
    response = client_rek.detect_labels(
        Image={
            'S3Object': {
                'Bucket': input['bucket'],
                'Name': input['key']
            }
        }
    )
    labels = [response['Labels'][i]['Name'].lower() for i in range(len(response['Labels']))] 
    return labels

def uploadToEs(input, labels):
    service = 'es'
    region = 'us-west-2'
    # headers = {"Content-Type" : "application/json"}
    credentials = boto3.Session().get_credentials()
    awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)
    mydata={
        "objectKey": input['key'],
        "bucket": input['bucket'],
        "createdTimestamp": datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
        "labels": labels
    }
    mydata_json = json.dumps(mydata)   
    es = Elasticsearch(
    hosts = [{'host': 'search-photos-ofe6jnxvlnksqy5bihubm67r7u.us-west-2.es.amazonaws.com', 'port': 443}],
    http_auth = awsauth,
    use_ssl = True,
    verify_certs = True,
    connection_class = RequestsHttpConnection
    )

    es.index(index="photos", doc_type="_doc", body=mydata_json)
    
def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']   
    input = {'bucket': bucket, 'key': key}
    custom_label = getImgMetaData(input)
    rek_labels = labelImg(input)
    rek_labels += custom_label
    print(rek_labels)
    uploadToEs(input, rek_labels)