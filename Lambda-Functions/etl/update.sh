#!/usr/bin/env bash

#update zip file
zip -g etl.zip etl.py

#send update to s3
aws s3 cp etl.zip s3://distribution-reliability-nlp/testing/etl.zip

#s3://distribution-reliability-nlp/testing/etl.zip

