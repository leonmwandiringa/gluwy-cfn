#!/bin/bash
echo "--------------------------------------------------------------------"
echo "--------------------------------------------------------------------"
echo "--------------------------------------------------------------------"
echo "------------------------ Gluwy Project -----------------------------"
echo "---------------- Setup project infrastructure-----------------------"
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

#create and setup s3 data lake
aws s3 mb s3://${PROJECT_NAME}  --region ${REGION}
aws s3api put-public-access-block --bucket ${PROJECT_NAME}\
  --public-access-block-configuration "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"
echo "${PROJECT_NAME} Created"

# copy all required files
aws s3 cp --recursive ./code/ s3://${PROJECT_NAME}/script/
aws s3 cp --recursive ./data/ s3://${PROJECT_NAME}/input/
aws s3 cp --recursive ./library/ s3://${PROJECT_NAME}/library/
aws s3 cp --recursive s3://covid19-lake/rearc-covid-19-testing-data/json/states_daily/ s3://${PROJECT_NAME}/input/lab5/json/

#run cfn stack
aws cloudformation create-stack --stack-name ${PROJECT_NAME} \
            --template-body file://gluestack.yaml \
            --capabilities CAPABILITY_NAMED_IAM \
            --region ${REGION} \
            --parameters \
            ParameterKey=UniquePostfix,ParameterValue=glueworkshop \
            ParameterKey=S3Bucket,ParameterValue=s3://${PROJECT_NAME}/