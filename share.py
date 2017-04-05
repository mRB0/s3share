#!/usr/bin/env python3

import logging as _logging
import argparse
import sys
from urllib.parse import quote as urlquote
import os
from mimetypes import guess_type
import json
import time

import boto3

import config

log = _logging.getLogger(__name__)

URL_SAFE_CHARS = frozenset({'-', '.', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '_', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'})

def make_urlsafe(s):
    l = [c if c in URL_SAFE_CHARS else '_'
         for c in s]
    
    return ''.join(l)

def load_state():
    home = os.getenv('HOME')
    assert home is not None, 'HOME must be set in environment'
    state_path = os.path.join(home, '.s3share')
    
    if not os.path.exists(state_path):
        state = {'entries': [],
                 'last_fetched_index': 0}
    else:
        with open(state_path, 'r', encoding='utf-8') as f:
            state = json.load(f)

    return state

def save_state(state):
    home = os.getenv('HOME')
    assert home is not None, 'HOME must be set in environment'
    state_path = os.path.join(home, '.s3share')
    
    with open(state_path, 'w', encoding='utf-8') as f:
        json.dump(state, f)
    
def update_state_from_table(state, table):
    new_items = table.query(ConsistentRead=True,
                            KeyConditionExpression=boto3.dynamodb.conditions.Key(config.AWS_DYNAMODB_PARTITION_KEY).eq("files") &
                            boto3.dynamodb.conditions.Key(config.AWS_DYNAMODB_SORT_KEY).gt(state['last_fetched_index']))

    log.info("update_state_from_table: fetched %d new items", len(new_items['Items']))
    
    for item in sorted(new_items['Items'], key=lambda i: int(i[config.AWS_DYNAMODB_SORT_KEY])):

        index = int(item[config.AWS_DYNAMODB_SORT_KEY])
        
        state['entries'].append({'s3_key': item['s3_key'],
                                 'uploaded': int(item['uploaded']),
                                 'index': index})

        state['last_fetched_index'] = max(state['last_fetched_index'], index)

    return state

def make_serve_url(key):
    serve_base_url = config.SERVE_BASE_URL or 'https://s3.{}.amazonaws.com/{}/'.format(config.AWS_S3_REGION, config.AWS_S3_BUCKET_NAME)
    return serve_base_url + urlquote(key)
    
def upload(paths):
    state = load_state()
    
    session = boto3.Session(aws_access_key_id=config.AWS_ACCESS_KEY_ID,
                            aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY)

    s3 = session.resource('s3', region_name=config.AWS_S3_REGION)
    db = session.resource('dynamodb', region_name=config.AWS_DYNAMODB_REGION)

    bucket = s3.Bucket(config.AWS_S3_BUCKET_NAME)

    urls = []
    s3_keys = []

    # upload objects
    
    for path in paths:
        filename = os.path.basename(path)

        if config.REPLACE_NON_URLSAFE_CHARACTERS_IN_FILENAMES:
            filename = make_urlsafe(filename)
        
        content_type = guess_type(filename)[0] or 'application-octet/stream'

        with open(path, 'rb') as f:
            log.info("Upload %s (%s)", path, content_type)
            
            obj = bucket.Object(filename)
            obj.put(ACL='public-read',
                    StorageClass='STANDARD_IA',
                    ContentType=content_type,
                    Body=f)

            s3_keys.append(filename)


        url = make_serve_url(filename)
        
        urls.append(url)

    
    for url in urls:
        print(url)

    # update dynamodb with new objects
    
    table = db.Table(config.AWS_DYNAMODB_TABLE)
    update_state_from_table(state, table)

    next_idx = state['last_fetched_index'] + 1
    for key in s3_keys:
        uploaded = int(time.time())
        
        table.put_item(ConditionExpression=boto3.dynamodb.conditions.Attr(config.AWS_DYNAMODB_SORT_KEY).not_exists(),
                       Item={config.AWS_DYNAMODB_PARTITION_KEY: 'files',
                             config.AWS_DYNAMODB_SORT_KEY: next_idx,
                             's3_key': key,
                             'uploaded': uploaded})

        state['entries'].append({'s3_key': key,
                                 'uploaded': uploaded,
                                 'index': next_idx})
        state['last_fetched_index'] = next_idx
        
        next_idx += 1
        
    save_state(state)

def list_entries():
    state = load_state()
    session = boto3.Session(aws_access_key_id=config.AWS_ACCESS_KEY_ID,
                            aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY)

    db = session.resource('dynamodb', region_name=config.AWS_DYNAMODB_REGION)
    table = db.Table(config.AWS_DYNAMODB_TABLE)
    
    update_state_from_table(state, table)

    for item in state['entries']:
        uploaded_formatted = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(item['uploaded']))
        url = make_serve_url(item['s3_key'])
        print("{}: {}".format(uploaded_formatted, url))
        
    save_state(state)

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-v', action='store_true', help='verbose')
    parser.add_argument('-d', action='store_true', help='debug')

    parser.add_argument('-l', action='store_true', help='List previous uploads')
    
    parser.add_argument('paths', metavar='PATH', type=str, nargs='*')
    args = parser.parse_args()

    if not (args.l or args.paths):
        parser.print_help()
        raise SystemExit(1)
    
    base_level = _logging.WARNING
    if args.v:
        base_level = _logging.INFO
    if args.d:
        base_level = _logging.DEBUG
        
    _logging.basicConfig(level=base_level, format="%(asctime)s %(levelname)-7s %(name)s %(message)s")
    
    if base_level != _logging.DEBUG:
        _logging.getLogger('boto3').setLevel(_logging.WARNING)
        _logging.getLogger('botocore').setLevel(_logging.WARNING)

    if args.paths:
        upload(args.paths)

    if args.l:
        list_entries()
    
