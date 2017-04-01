#!/usr/bin/env python3

# RUN ONCE to initialize a dynamodb table from a bucket's existing entries

# aws s3 ls <bucket> | import_objects

import time
import sys
import re

files = sys.stdin.readlines()

entries = []

for line in files:
    line = line.strip()

    match = re.match(r'^([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}) +([0-9]+) (.*)$', line)
    uploaded, size, filename = match.groups()

    timetup = time.strptime(uploaded, '%Y-%m-%d %H:%M:%S')
    unixtime = int(time.mktime(timetup))

    entries.append([unixtime, filename])

entries.sort(key=lambda e: e[0])
    
import boto3
import config

session = boto3.Session(aws_access_key_id=config.AWS_ACCESS_KEY_ID,
                        aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY)

db = session.resource('dynamodb', region_name=config.AWS_DYNAMODB_REGION)
table = db.Table(config.AWS_DYNAMODB_TABLE)


with table.batch_writer() as writer:
    for i, (unixtime, filename) in enumerate(entries):

        print("{}. {}: {!r}".format(i + 1, unixtime, filename))
    
        writer.put_item(Item={config.AWS_DYNAMODB_PARTITION_KEY: 'files',
                              config.AWS_DYNAMODB_SORT_KEY: i + 1,
                              's3_key': filename,
                              'uploaded': unixtime})
        
