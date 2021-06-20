#!/bin/bash
echo "--------------------------------------------------------------------"
echo "--------------------------------------------------------------------"
echo "--------------------------------------------------------------------"
echo "------------------------ Gluwy Project -----------------------------"
echo "------------------- Setup project glue job -------------------------"
echo "--------------------------------------------------------------------"
echo "--------------------------------------------------------------------"
echo "--------------------------------------------------------------------
"

if [ -z "$1" ]
    then
        echo "project name is required"
        sleep 5s
        exit 0
fi

if [ -z "$2" ]
    then
        echo "Region is required"
        sleep 5s
        exit 0
fi
PROJECT_NAME=$1
REGION=$2

aws glue create-job \
    --name${PROJECT_NAME}-etl-job \
    --role AWSGlueServiceRole-glueworkshop \
    --command "Name=glueetl,ScriptLocation=s3://${PROJECT_NAME}/script/lab3/spark.py,PythonVersion=3" \
    --glue-version '2.0' \
    --default-arguments "{\"--extra-py-files\": \"s3://${PROJECT_NAME}/library/pycountry_convert.zip\", \
        \"--s3_bucket\": \"s3://${PROJECT_NAME}/\" }" \
    --region ${REGION}