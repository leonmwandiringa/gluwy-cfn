import sys
from datetime import datetime
import boto3
import base64
from pyspark.sql import DataFrame, Row
from pyspark.context import SparkContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME', 's3_bucket'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# S3 sink locations
output_path = args['s3_bucket'] + "/output/lab4/"
job_time_string = datetime.now().strftime("%Y%m%d%H%M%S")
s3_target = output_path + job_time_string
checkpoint_location = output_path + "checkpoint/"
temp_path = output_path + "temp/"

country_lookup_path = args['s3_bucket'] + "/input/lab4/country_lookup/"
country_lookup_frame = glueContext.create_dynamic_frame.from_options( \
                            format_options = {"withHeader":True, "separator":",", "quoteChar":"\""}, \
                            connection_type = "s3", \
                            format = "csv", \
                            connection_options = {"paths": [country_lookup_path], "recurse":True}, \
                            transformation_ctx = "country_lookup_frame")

def processBatch(data_frame, batchId):
    if (data_frame.count() > 0):
        dynamic_frame = DynamicFrame.fromDF(data_frame, glueContext, "from_data_frame")
        apply_mapping = ApplyMapping.apply(frame = dynamic_frame, mappings = [ \
            ("uuid", "string", "uuid", "bigint"), \
            ("country", "string", "country", "string"), \
            ("item type", "string", "item type", "string"), \
            ("sales channel", "string", "sales channel", "string"), \
            ("order priority", "string", "order priority", "string"), \
            ("order date", "string", "order date", "string"), \
            ("region", "string", "region", "string"), \
            ("ship date", "string", "ship date", "string"), \
            ("units sold", "int", "units sold", "int"), \
            ("unit price", "string", "unit price", "decimal"), \
            ("unit cost", "string", "unit cost", "decimal"), \
            ("total revenue", "string", "total revenue", "decimal"), \
            ("total cost", "string", "total cost", "decimal"), \
            ("total profit", "string", "total profit", "decimal")],\
            transformation_ctx = "apply_mapping")

        final_frame = Join.apply(apply_mapping, country_lookup_frame, 'country', 'CountryName').drop_fields( \
                ['CountryName', 'country', 'unit price', 'unit cost', 'total revenue', 'total cost', 'total profit'])

        s3sink = glueContext.write_dynamic_frame.from_options(  frame = final_frame, \
                                                                connection_type = "s3", \
                                                                connection_options = {"path": s3_target}, \
                                                                format = "csv", \
                                                                transformation_ctx = "s3sink")

# Read from Kinesis Data Stream from catalog table
sourceData = glueContext.create_data_frame.from_catalog( \
    database = "glueworkshop-cloudformation", \
    table_name = "json-streaming-table", \
    transformation_ctx = "datasource0", \
    additional_options = {"startingPosition": "TRIM_HORIZON", "inferSchema": "true"})

glueContext.forEachBatch(frame = sourceData, \
                        batch_function = processBatch, \
                        options = {"windowSize": "60 seconds", "checkpointLocation": checkpoint_location})
job.commit()