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
"""
This is a more advanced example dag for using `GoogleApiToS3Operator` which uses xcom to pass data between
tasks to retrieve specific information about YouTube videos:

First it searches for up to 50 videos (due to pagination) in a given time range
(YOUTUBE_VIDEO_PUBLISHED_AFTER, YOUTUBE_VIDEO_PUBLISHED_BEFORE) on a YouTube channel (YOUTUBE_CHANNEL_ID)
saves the response in S3 + passes over the YouTube IDs to the next request which then gets the information
(YOUTUBE_VIDEO_FIELDS) for the requested videos and saves them in S3 (S3_DESTINATION_KEY).

Further information:

YOUTUBE_VIDEO_PUBLISHED_AFTER and YOUTUBE_VIDEO_PUBLISHED_BEFORE needs to be formatted
``YYYY-MM-DDThh:mm:ss.sZ``.
See https://developers.google.com/youtube/v3/docs/search/list for more information.
YOUTUBE_VIDEO_PARTS depends on the fields you pass via YOUTUBE_VIDEO_FIELDS. See
https://developers.google.com/youtube/v3/docs/videos/list#parameters for more information.
YOUTUBE_CONN_ID is optional for public videos. It does only need to authenticate when there are private videos
on a YouTube channel you want to retrieve.

Authentication:
In order for the DAG to run, the GoogleApiToS3Operator needs to authenticate to Google Cloud APIs.
Further information about authenticating can be found:
https://cloud.google.com/docs/authentication/getting-started
https://cloud.google.com/docs/authentication/provide-credentials-adc

https://airflow.apache.org/docs/apache-airflow-providers-google/stable/connections/gcp.html

The required scope for this DAG is https://www.googleapis.com/auth/youtube.readonly.
This can be set via the environment variable AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT,
or by creating a custom connection.
"""

from __future__ import annotations

import json
from datetime import datetime

import boto3

from airflow import settings
from airflow.decorators import task
from airflow.models import Connection
from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.operators.s3 import (
    S3CreateBucketOperator,
    S3DeleteBucketOperator,
)
from airflow.providers.amazon.aws.transfers.google_api_to_s3 import GoogleApiToS3Operator
from airflow.utils.trigger_rule import TriggerRule

from providers.tests.system.amazon.aws.utils import ENV_ID_KEY, SystemTestContextBuilder

DAG_ID = "example_google_api_youtube_to_s3"

YOUTUBE_CHANNEL_ID = "UCSXwxpWZQ7XZ1WL3wqevChA"
YOUTUBE_VIDEO_PUBLISHED_AFTER = "2019-09-25T00:00:00Z"
YOUTUBE_VIDEO_PUBLISHED_BEFORE = "2019-10-18T00:00:00Z"
YOUTUBE_VIDEO_PARTS = "snippet"
YOUTUBE_VIDEO_FIELDS = "items(id,snippet(description,publishedAt,tags,title))"

SECRET_ARN_KEY = "SECRET_ARN"

sys_test_context_task = SystemTestContextBuilder().add_variable(SECRET_ARN_KEY).build()


@task
def create_connection_gcp(conn_id_name: str, secret_arn: str):
    json_data = boto3.client("secretsmanager").get_secret_value(SecretId=secret_arn)[
        "SecretString"
    ]
    conn = Connection(
        conn_id=conn_id_name,
        conn_type="google_cloud_platform",
    )
    scopes = "https://www.googleapis.com/auth/youtube.readonly"
    conn_extra = {
        "scope": scopes,
        "project": "aws-oss-airflow",
        "keyfile_dict": json_data,
    }
    conn_extra_json = json.dumps(conn_extra)
    conn.set_extra(conn_extra_json)
    session = settings.Session()
    session.add(conn)
    session.commit()


@task(task_id="wait_for_s3_bucket")
def wait_for_bucket(s3_bucket_name):
    waiter = boto3.client("s3").get_waiter("bucket_exists")
    waiter.wait(Bucket=s3_bucket_name)


@task(task_id="transform_video_ids")
def transform_video_ids(**kwargs):
    task_instance = kwargs["task_instance"]
    output = task_instance.xcom_pull(task_ids="video_ids_to_s3", key="video_ids_response")
    video_ids = [item["id"]["videoId"] for item in output["items"]]

    if not video_ids:
        video_ids = []

    kwargs["task_instance"].xcom_push(key="video_ids", value={"id": ",".join(video_ids)})


with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),  # Override to match your needs
    tags=["example"],
    catchup=False,
) as dag:
    test_context = sys_test_context_task()
    env_id = test_context[ENV_ID_KEY]
    conn_id_name = f"{env_id}-conn-id"
    secret_arn = test_context[SECRET_ARN_KEY]

    set_up_connection = create_connection_gcp(conn_id_name, secret_arn=secret_arn)

    s3_bucket_name = f"{env_id}-bucket"

    create_s3_bucket = S3CreateBucketOperator(
        task_id="create-s3-bucket", bucket_name=s3_bucket_name
    )

    wait_for_bucket_creation = wait_for_bucket(s3_bucket_name=s3_bucket_name)

    # [START howto_transfer_google_api_youtube_search_to_s3]
    video_ids_to_s3 = GoogleApiToS3Operator(
        task_id="video_ids_to_s3",
        google_api_service_name="youtube",
        google_api_service_version="v3",
        google_api_endpoint_path="youtube.search.list",
        gcp_conn_id=conn_id_name,
        google_api_endpoint_params={
            "part": "snippet",
            "channelId": YOUTUBE_CHANNEL_ID,
            "maxResults": 50,
            "publishedAfter": YOUTUBE_VIDEO_PUBLISHED_AFTER,
            "publishedBefore": YOUTUBE_VIDEO_PUBLISHED_BEFORE,
            "type": "video",
            "fields": "items/id/videoId",
        },
        google_api_response_via_xcom="video_ids_response",
        s3_destination_key=f"https://s3.us-west-2.amazonaws.com/{s3_bucket_name}/youtube_search",
        s3_overwrite=True,
    )
    # [END howto_transfer_google_api_youtube_search_to_s3]

    transform_video_ids_task = transform_video_ids()

    # [START howto_transfer_google_api_youtube_list_to_s3]
    video_data_to_s3 = GoogleApiToS3Operator(
        task_id="video_data_to_s3",
        google_api_service_name="youtube",
        google_api_service_version="v3",
        gcp_conn_id=conn_id_name,
        google_api_endpoint_path="youtube.videos.list",
        google_api_endpoint_params={
            "part": YOUTUBE_VIDEO_PARTS,
            "maxResults": 50,
            "fields": YOUTUBE_VIDEO_FIELDS,
        },
        google_api_endpoint_params_via_xcom="video_ids",
        s3_destination_key=f"https://s3.us-west-2.amazonaws.com/{s3_bucket_name}/youtube_videos",
        s3_overwrite=True,
    )
    # [END howto_transfer_google_api_youtube_list_to_s3]

    delete_s3_bucket = S3DeleteBucketOperator(
        task_id="delete_s3_bucket",
        bucket_name=s3_bucket_name,
        force_delete=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    chain(
        # TEST SETUP
        test_context,
        set_up_connection,
        create_s3_bucket,
        wait_for_bucket_creation,
        # TEST BODY
        video_ids_to_s3,
        transform_video_ids_task,
        video_data_to_s3,
        # TEST TEARDOWN
        delete_s3_bucket,
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
