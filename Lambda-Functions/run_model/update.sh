#!/usr/bin/env bash

#update zip file
zip -g run_model.zip run_model.py

#send update to s3
aws s3 cp run_model.zip s3://distribution-reliability-nlp/testing/run_model.zip

#s3://distribution-reliability-nlp/testing/run_model.zip

