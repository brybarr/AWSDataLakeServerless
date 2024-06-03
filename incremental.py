import urllib3
import sys
import json
import logging
import asyncio
import boto3
import os
import time
import datetime
import pymysql

import pandas as pd
#import awswrangler as wr
from pyspark.sql.functions import unix_timestamp
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from datetime import datetime, timedelta, timezone
from pyspark.sql import functions as F
from awsglue.job import Job
from pprint import pprint
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError



MSG_FORMAT = '%(message)s'
logging.basicConfig(format = MSG_FORMAT)
logger = logging.getLogger()
logger.setLevel('INFO')
session = boto3.Session(region_name='ap-southeast-1')
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

jobname = args['JOB_NAME']
DATA_SOURCE_NAME = 'LEAD-API-DB-DELTA'
DYNAMO_CONFIG_TABLE_NAME = 'poc-process-table'

os.environ['TZ'] = 'Asia/Manila'
time.tzset()
current_timestamp = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
startTimestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

batch_year = (current_timestamp.now().strftime("%Y"))
batch_month = (current_timestamp.now().strftime("%m"))
batch_day = (current_timestamp.now().strftime("%d"))

secretsmanager = session.client(service_name='secretsmanager')
secret_response = secretsmanager.get_secret_value(SecretId='rds-secret-dev')
secret = json.loads(secret_response['SecretString'])


def sns_publisher(topic_arn, msg, sub):
    """
    send notification base on different events

    @input: topic arn, message, subject
    @output: None
    """

    try:
        sns.publish(
            TopicArn=topic_arn,
            Message=msg,
            Subject=sub
        )
    except ClientError as error:
        logger.error("The error occurred when publishing SNS")
        logger.exception(error)
        raise error


def eventProcessDetails(p1, p2, p3, p4, p5, p6):
    """
    print process details in cloudwatch
    """
    try:
        eventProcess = (f'eventProcessDetails:{{"ProcessId":"{p1}", "DataSource":"{p2}", "TargetTable":"{p3}", "ProcessCount":"{p4}", "StartTimestamp":"{p5}", "EndTimestamp":"{p6}"}}')
        return eventProcess
    except Exception as error:
        logger.error("The error occurred when publishing SNS")
        logger.exception(error)
        raise error


def getParametersFromDynamoDB():
    try:
        dynamodb = session.resource('dynamodb')
        table = dynamodb.Table(DYNAMO_CONFIG_TABLE_NAME)
        rows = table.scan(
            ScanFilter={
                'DataSourceName': {
                    'AttributeValueList': [DATA_SOURCE_NAME],
                    'ComparisonOperator': 'EQ'
                }
            }
        )
        if not rows['Count']:
            raise ValueError('No rows for the datasource name specified.')

        return rows['Items']
    except Exception as error:
        logger.error(error)
        raise ValueError(error)
        
def createGlueTable(df, job):
    try:
        ddf = DynamicFrame.fromDF(df, glueContext, 'ddf')
        ddf = ddf.repartition(10)
        sink = glueContext.getSink(
            connection_type="s3",
            path=f's3://{job["TargetBucket"]}/{job["TargetPrefix"]}/{job["TargetTable"]}/',
            enableUpdateCatalog=True,
            updateBehavior="UPDATE_IN_DATABASE",
            partitionKeys=["batch_year", "batch_month", "batch_day"]
        )
        sink.setFormat("glueparquet")
        sink.setCatalogInfo(
            catalogDatabase=job["TargetDB"],
            catalogTableName=job["TargetTable"]
        )
        sink.writeFrame(ddf)
        
        print(f"{job['ProcessId']} process done.")
    except Exception as error:
        logger.error(error)
        raise ValueError('Error while create glue table.', error)

def loadTask(job):
    try:
        rowCount=0
        if job["RuntimeMode"].upper() == 'FULL_LOAD':
            print('Full Load')
            print(f'***** Job Starting Full load Ingestion *****')
            glueDF = glueContext.create_dynamic_frame.from_catalog(
                database=job["TargetDB"],
                table_name=job["SourceTable"],
                transformation_ctx="transform",
            )
            
            df=glueDF.toDF()

            df = df.withColumn('UploadedDatetime',F.lit(current_timestamp.strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp")) \
                  .withColumn('Src_Sys_Cd',F.lit(job["DataSourceName"])) \
                  .withColumn('ProcessId',F.lit(job["ProcessId"]))  \
                  .withColumn('created_date',F.to_timestamp('created_date', 'yyyy-MM-dd HH:mm:ss')) \
                  .withColumn('modified_date',F.to_timestamp('modified_date', 'yyyy-MM-dd HH:mm:ss')) \
                  .withColumn('batch_year',F.year(F.to_timestamp('created_date', 'yyyy-MM-dd HH:mm:ss'))) \
                  .withColumn('batch_month',F.month(F.to_timestamp('created_date', 'yyyy-MM-dd HH:mm:ss'))) \
                  .withColumn('batch_day',F.dayofmonth(F.to_timestamp('created_date', 'yyyy-MM-dd HH:mm:ss')))
            
            
            print(f'***** Start Writing to S3 *****')
            createGlueTable(df, job)
            print(f'***** Finished Writing to S3 *****')
            
        elif job["RuntimeMode"].upper() == 'DELTA':
            print('Incremental')
            print(f'***** Job Starting Delta load Ingestion *****')
            
            glueDF = glueContext.create_dynamic_frame.from_catalog(
                database=job["TargetDB"],
                table_name=job["SourceTable"],
                transformation_ctx="transform",
            )
            
            dfg=glueDF.toDF()  
              
            dfg.createOrReplaceTempView('delta')  
            # date_created = spark.sql(f"select cast(max({job['CheckPointCol']}) as string) as date_created from {job['TargetDB']}.{job['TargetTable']}").first()["date_created"]
            date_created=spark.sql(f"select upper(cast(date_format(cast(max({job['CheckPointCol']}) as timestamp), 'yyyy-MM-dd HH:mm:ss') as varchar(20))) as date_created from {job['TargetDB']}.{job['TargetTable']}").first()["date_created"]
            print(date_created)
            
            
            print('Executing: ', job["SqlQueryTxn"])

            print(f'***** Starting Data Extraction *****')
            
            df=spark.sql(eval(job["SqlQueryTxn"]))
                
            print(f'***** Data Extractted to Dataframe *****')
            
        
            df = df.withColumn('UploadedDatetime',F.lit(current_timestamp.strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp")) \
                   .withColumn('SRC_SYS_CD',F.lit(job["DataSourceName"])) \
                   .withColumn('ProcessId',F.lit(job["ProcessId"])) \
                   .withColumn('batch_year',F.year(F.to_timestamp('created_date', 'yyyy-MM-dd HH:mm:ss'))) \
                   .withColumn('batch_month',F.month(F.to_timestamp('created_date', 'yyyy-MM-dd HH:mm:ss'))) \
                   .withColumn('batch_day',F.dayofmonth(F.to_timestamp('created_date', 'yyyy-MM-dd HH:mm:ss')))
            
            rowCount = df.count()
            print('Row count is: ', rowCount)
            print(f'***** Start Writing to S3 *****')
            createGlueTable(df, job)
            print(f'***** Finished Writing to S3 *****')
        
        endTimestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")          
        logger.info('\n')
        logger.info(eventProcessDetails(job["ProcessId"], job["DataSourceName"], job["TargetTable"], rowCount, startTimestamp, endTimestamp))    
        logger.info('\n')
    except Exception as error:
        logger.error(error)
        msg = f"Glue jog execution for loading failed\n" + \
              f"Error Message: {error}"
        sub = f"AWS Glue Job Execution for {jobname}"
        #sns_publisher(topic_arn, msg, sub)
        raise error


async def processTask(loop, job):
    try:
        await loop.run_in_executor(None, loadTask, job)
    except Exception as error:
        logger.error(error)
        raise ValueError(f'Error while processing job for {job["TargetTable"]} in processTask')

def main():
    try:
        gluejob = Job(glueContext)
        jobs = getParametersFromDynamoDB()
        loop = asyncio.get_event_loop()
        tasks = []
        logger.info(f'***** Job Start *****')
        for job in jobs:
            print(f'extracting {job["SourceTable"]}')
            loadTask(job)
        #loop.run_until_complete(asyncio.wait(tasks))
        #logger.info('***** All Jobs Completed *****')
          
    except Exception as error:
        logger.error(error)
        msg = f"Glue jog execution for loading failed\n" + \
              f"Error Message: {error}"
        sub = f"AWS Glue Job Execution for {jobname}"
        #sns_publisher(topic_arn, msg, sub)
        raise error

if __name__ == '__main__':
    main()
