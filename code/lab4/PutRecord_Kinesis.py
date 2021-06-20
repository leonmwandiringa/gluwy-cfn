import csv
import json
import boto3
import time

def generate(stream_name, kinesis_client):
    with open("./data/lab2/sample.csv", encoding='utf-8') as csvf:
        csvReader = csv.DictReader(csvf)
        for rows in csvReader:
            #debug info in console
            print(json.dumps(rows))
            time.sleep(0.2)
            kinesis_client.put_record(StreamName=stream_name,
                                      Data=json.dumps(rows),
                                      PartitionKey="partitionkey")

if __name__ == '__main__':
    generate('glueworkshop', boto3.client('kinesis'))
