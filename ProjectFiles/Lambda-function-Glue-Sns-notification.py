# Databricks notebook source
# MAGIC %md
# MAGIC #### Lambda function that triggers the Glue job and the Sns notification

# COMMAND ----------

import json
import boto3


def lambda_handler(event, context):

    bucketName = event["Records"][0]["s3"]["bucket"]["name"]
    fileName = event["Records"][0]["s3"]["object"]["key"]

    print("Bucket Name:", bucketName, "File Name:", fileName)
    glue = boto3.client("glue")

    response = glue.start_job_run(
        JobName="glue-cdc-pyspark",
        Arguments={"--s3_source_bucket": bucketName, "--s3_source_file": fileName},
    )

    sns = boto3.client("sns")

    # Set your SNS topic ARN
    sns_topic_arn = "......"

    # Check if Glue job succeeded (you can add more conditions based on your needs)
    job_state = event["detail"]["state"]
    job_name = event["detail"]["jobName"]

    if job_state == "SUCCEEDED":
        # Send success notification
        message = f"The Glue job {job_name} has successfully completed."
        sns.publish(
            TopicArn=sns_topic_arn,
            Message=message,
            Subject="Glue Job Completed Successfully",
        )
    elif job_state == "FAILED":
        message = f"The Glue job {job_name} failed."
        sns.publish(TopicArn=sns_topic_arn, Message=message, Subject="Glue Job Failed")
    else:
        message = f"The Glue job {job_name} is in state {job_state}."
        sns.publish(
            TopicArn=sns_topic_arn, Message=message, Subject="Glue Job State Change"
        )

    return {"statusCode": 200, "body": json.dumps("SNS notification sent")}
