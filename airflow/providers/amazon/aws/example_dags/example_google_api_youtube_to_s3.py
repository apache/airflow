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
"""

from datetime import datetime
from os import getenv

from airflow import DAG
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.transfers.google_api_to_s3 import GoogleApiToS3Operator

YOUTUBE_CHANNEL_ID = getenv(
    "YOUTUBE_CHANNEL_ID", "UCSXwxpWZQ7XZ1WL3wqevChA"
)  # Youtube channel "Apache Airflow"
YOUTUBE_VIDEO_PUBLISHED_AFTER = getenv("YOUTUBE_VIDEO_PUBLISHED_AFTER", "2019-09-25T00:00:00Z")
YOUTUBE_VIDEO_PUBLISHED_BEFORE = getenv("YOUTUBE_VIDEO_PUBLISHED_BEFORE", "2019-10-18T00:00:00Z")
S3_BUCKET_NAME = getenv("S3_DESTINATION_KEY", "s3://bucket-test")
YOUTUBE_VIDEO_PARTS = getenv("YOUTUBE_VIDEO_PARTS", "snippet")
YOUTUBE_VIDEO_FIELDS = getenv("YOUTUBE_VIDEO_FIELDS", "items(id,snippet(description,publishedAt,tags,title))")


@task(task_id='transform_video_ids')
def transform_video_ids(**kwargs):
    task_instance = kwargs['task_instance']
    output = task_instance.xcom_pull(task_ids="video_ids_to_s3", key="video_ids_response")
    video_ids = [item['id']['videoId'] for item in output['items']]

    if not video_ids:
        video_ids = []

    kwargs['task_instance'].xcom_push(key='video_ids', value={'id': ','.join(video_ids)})


with DAG(
    dag_id="example_google_api_youtube_to_s3",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    # [START howto_transfer_google_api_youtube_search_to_s3]
    task_video_ids_to_s3 = GoogleApiToS3Operator(
        task_id='video_ids_to_s3',
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
            'fields': 'items/id/videoId',
        },
        google_api_response_via_xcom='video_ids_response',
        s3_destination_key=f'{S3_BUCKET_NAME}/youtube_search.json',
        s3_overwrite=True,
    )
    # [END howto_transfer_google_api_youtube_search_to_s3]

    task_transform_video_ids = transform_video_ids()

    # [START howto_transfer_google_api_youtube_list_to_s3]
    task_video_data_to_s3 = GoogleApiToS3Operator(
        task_id='video_data_to_s3',
        google_api_service_name='youtube',
        google_api_service_version='v3',
        google_api_endpoint_path='youtube.videos.list',
        google_api_endpoint_params={
            'part': YOUTUBE_VIDEO_PARTS,
            'maxResults': 50,
            'fields': YOUTUBE_VIDEO_FIELDS,
        },
        google_api_endpoint_params_via_xcom='video_ids',
        s3_destination_key=f'{S3_BUCKET_NAME}/youtube_videos.json',
        s3_overwrite=True,
    )
    # [END howto_transfer_google_api_youtube_list_to_s3]

    chain(task_video_ids_to_s3, task_transform_video_ids, task_video_data_to_s3)
