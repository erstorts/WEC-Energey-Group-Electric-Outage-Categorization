#!/usr/bin/env bash

pip3 install --target ./package datetime

#pip3 install --target ./package 'urllib3<1.25,>=1.21'

#pip3 install --target ./package xlrd

#pip3 install --target ./package numpy

pip3 install --target ./package pandas

#pip3 install --target ./package boto3

pip3 install --target ./package sklearn


cd package

zip -r9 ${OLDPWD}/run_model.zip .

cd $OLDPWD

zip -g run_model.zip run_model.py

