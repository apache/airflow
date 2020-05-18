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
This is a more advanced example dag for using `GoogleApiToS3Transfer` which uses xcom to pass data between
tasks to retrieve specific information about YouTube videos:

First it searches for up to 50 videos (due to pagination) in a given time range
(YOUTUBE_VIDEO_PUBLISHED_AFTER, YOUTUBE_VIDEO_PUBLISHED_BEFORE) on a YouTube channel (YOUTUBE_CHANNEL_ID)
saves the response in S3 + passes over the YouTube IDs to the next request which then gets the information
(YOUTUBE_VIDEO_FIELDS) for the requested videos and saves them in S3 (S3_DESTINATION_KEY).

Further information:

YOUTUBE_VIDEO_PUBLISHED_AFTER and YOUTUBE_VIDEO_PUBLISHED_BEFORE needs to be formatted
"YYYY-MM-DDThh:mm:ss.sZ". See https://developers.google.com/youtube/v3/docs/search/list for more information.
YOUTUBE_VIDEO_PARTS depends on the fields you pass via YOUTUBE_VIDEO_FIELDS. See
https://developers.google.com/youtube/v3/docs/videos/list#parameters for more information.
YOUTUBE_CONN_ID is optional for public videos. It does only need to authenticate when there are private videos
on a YouTube channel you want to retrieve.
"""

from os import getenv

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.amazon.aws.operators.google_api_to_s3_transfer import GoogleApiToS3Transfer
from airflow.utils.dates import days_ago

# [START howto_operator_google_api_to_s3_transfer_advanced_env_variables]
YOUTUBE_CONN_ID = getenv("YOUTUBE_CONN_ID", "google_cloud_default")
YOUTUBE_CHANNEL_ID = getenv("YOUTUBE_CHANNEL_ID", "UCSXwxpWZQ7XZ1WL3wqevChA")  # "Apache Airflow"
YOUTUBE_VIDEO_PUBLISHED_AFTER = getenv("YOUTUBE_VIDEO_PUBLISHED_AFTER", "2019-09-25T00:00:00Z")
YOUTUBE_VIDEO_PUBLISHED_BEFORE = getenv("YOUTUBE_VIDEO_PUBLISHED_BEFORE", "2019-10-18T00:00:00Z")
S3_DESTINATION_KEY = getenv("S3_DESTINATION_KEY", "s3://bucket/key.json")
YOUTUBE_VIDEO_PARTS = getenv("YOUTUBE_VIDEO_PARTS", "snippet")
YOUTUBE_VIDEO_FIELDS = getenv("YOUTUBE_VIDEO_FIELDS", "items(id,snippet(description,publishedAt,tags,title))")
# [END howto_operator_google_api_to_s3_transfer_advanced_env_variables]

default_args = {"start_date": days_ago(1)}


# pylint: disable=unused-argument
# [START howto_operator_google_api_to_s3_transfer_advanced_task_1_2]
def _check_and_transform_video_ids(xcom_key, task_ids, task_instance, **kwargs):
    video_ids_response = task_instance.xcom_pull(task_ids=task_ids, key=xcom_key)
    video_ids = [item['id']['videoId'] for item in video_ids_response['items']]

    if video_ids:
        task_instance.xcom_push(key='video_ids', value={'id': ','.join(video_ids)})
        return 'video_data_to_s3'
    return 'no_video_ids'


# [END howto_operator_google_api_to_s3_transfer_advanced_task_1_2]
# pylint: enable=unused-argument

s3_directory, s3_file = S3_DESTINATION_KEY.rsplit('/', 1)
s3_file_name, _ = s3_file.rsplit('.', 1)

with DAG(
    dag_id="example_google_api_to_s3_transfer_advanced",
    default_args=default_args,
    schedule_interval=None,
    tags=['example']
) as dag:
    # [START howto_operator_google_api_to_s3_transfer_advanced_task_1]
    task_video_ids_to_s3 = GoogleApiToS3Transfer(
        gcp_conn_id=YOUTUBE_CONN_ID,
        google_api_service_name='youtube',
        google_api_service_version='v3',
        google_api_endpoint_path='youtube.search.list',
        google_api_endpoint_params={
            'part': 'snippet',
            'channelId': YOUTUBE_CHANNEL_ID,
            'maxResults': 50,
            'publishedAfter': YOUTUBE_VIDEO_PUBLISHED_AFTER,
            'publishedBefore': YOUTUBE_VIDEO_PUBLISHED_BEFORE,
            'type': 'video',
            'fields': 'items/id/videoId'
        },
        google_api_response_via_xcom='video_ids_response',
        s3_destination_key=f'{s3_directory}/youtube_search_{s3_file_name}.json',
        task_id='video_ids_to_s3'
    )
    # [END howto_operator_google_api_to_s3_transfer_advanced_task_1]
    # [START howto_operator_google_api_to_s3_transfer_advanced_task_1_1]
    task_check_and_transform_video_ids = BranchPythonOperator(
        python_callable=_check_and_transform_video_ids,
        op_args=[
            task_video_ids_to_s3.google_api_response_via_xcom,
            task_video_ids_to_s3.task_id
        ],
        task_id='check_and_transform_video_ids'
    )
    # [END howto_operator_google_api_to_s3_transfer_advanced_task_1_1]
    # [START howto_operator_google_api_to_s3_transfer_advanced_task_2]
    task_video_data_to_s3 = GoogleApiToS3Transfer(
        gcp_conn_id=YOUTUBE_CONN_ID,
        google_api_service_name='youtube',
        google_api_service_version='v3',
        google_api_endpoint_path='youtube.videos.list',
        google_api_endpoint_params={
            'part': YOUTUBE_VIDEO_PARTS,
            'maxResults': 50,
            'fields': YOUTUBE_VIDEO_FIELDS
        },
        google_api_endpoint_params_via_xcom='video_ids',
        s3_destination_key=f'{s3_directory}/youtube_videos_{s3_file_name}.json',
        task_id='video_data_to_s3'
    )
    # [END howto_operator_google_api_to_s3_transfer_advanced_task_2]
    # [START howto_operator_google_api_to_s3_transfer_advanced_task_2_1]
    task_no_video_ids = DummyOperator(
        task_id='no_video_ids'
    )
    # [END howto_operator_google_api_to_s3_transfer_advanced_task_2_1]
    task_video_ids_to_s3 >> task_check_and_transform_video_ids >> [task_video_data_to_s3, task_no_video_ids]
