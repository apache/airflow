# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import json
import logging
import os
import subprocess
from tempfile import mkdtemp

import boto3

"""
Example Lambda function to execute an Airflow command or workload. Use or modify this code as needed.
"""

log = logging.getLogger()
log.setLevel(logging.INFO)


# Get the S3 URI from the environment variable. Set either on the Lambda function or in the
# docker image used for the lambda invocations.
S3_URI = os.environ.get("S3_URI", None)
# Input and output keys
TASK_KEY_KEY = "task_key"
COMMAND_KEY = "command"
EXECUTOR_CONFIG_KEY = "executor_config"
RETURN_CODE_KEY = "return_code"


def lambda_handler(event, context):
    log.info("Received event: %s", event)
    log.info("Received context: %s", context)

    command = event.get(COMMAND_KEY)
    task_key = event.get(TASK_KEY_KEY)
    executor_config = event.get(EXECUTOR_CONFIG_KEY, {})  # noqa: F841

    # Any pre-processing or validation of the command or use of the executor_config can be done here or above.

    # Sync dags from s3 to the local dags directory
    if S3_URI:
        fetch_dags_from_s3(S3_URI)
    # This function must be called, it executes the Airflow command and reports to SQS.
    run_and_report(command, task_key)

    # Any post-processing or cleanup can be done here.


def run_and_report(command, task_key):
    """Execute the provided Airflow command or workload and report the result via SQS."""
    try:
        log.info("Starting execution for task: %s", task_key)
        result = subprocess.run(
            command,
            check=False,
            shell=isinstance(command, str),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        return_code = result.returncode
        log.info("Execution completed for task %s with return code %s", task_key, return_code)
        log.info("Output:")
        log.info("%s", result.stdout.decode())
    except Exception:
        log.exception("Error executing task %s: ", task_key)
        return_code = 1  # Non-zero indicates failure to run the task

    queue_url = get_queue_url()
    message = json.dumps({TASK_KEY_KEY: task_key, RETURN_CODE_KEY: return_code})
    try:
        sqs_client = get_sqs_client()
        sqs_client.send_message(QueueUrl=queue_url, MessageBody=message)
        log.info("Sent result to SQS %s", message)
    except Exception:
        log.exception("Failed to send message to SQS for task %s", task_key)


def get_sqs_client():
    """Create an SQS client. Credentials and region are automatically picked up from the environment."""
    return boto3.client("sqs")


def get_queue_url():
    """
    Get the SQS queue URL from the environment variable.

    Set either on the Lambda function or in the image used for the lambda invocations.
    """
    queue_url = os.environ.get("AIRFLOW__AWS_LAMBDA_EXECUTOR__QUEUE_URL", os.environ.get("QUEUE_URL", None))
    if not queue_url:
        raise RuntimeError(
            "No Queue URL detected (either AIRFLOW__AWS_LAMBDA_EXECUTOR__QUEUE_URL or "
            "QUEUE_URL); Will be unable to send task results. Exiting!"
        )
    return queue_url


def fetch_dags_from_s3(s3_uri):
    """Fetch DAGs from S3 and sync them to the local dags directory."""
    log.info("Fetching DAGs from S3 URI: %s", s3_uri)
    # Use a named temporary directory for the local dags folder, only tmp is writeable in Lambda
    local_dags_dir = mkdtemp(prefix="airflow_dags_")
    log.info("Setting AIRFLOW__CORE__DAGS_FOLDER to: %s", local_dags_dir)
    os.environ["AIRFLOW__CORE__DAGS_FOLDER"] = local_dags_dir

    # S3 URI format s3://bucket-name/path/to/dags/
    bucket_name = s3_uri.split("/")[2]
    prefix = "/".join(s3_uri.split("/")[3:])

    s3_resource = boto3.resource("s3")
    bucket = s3_resource.Bucket(bucket_name)

    for obj in bucket.objects.filter(Prefix=prefix):
        if obj.key.endswith("/"):
            # Skip directories
            continue
        key = obj.key
        local_path = os.path.join(local_dags_dir, os.path.basename(key))
        log.info("Downloading %s to %s", key, local_path)
        bucket.download_file(key, local_path)
