 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

:py:mod:`tests.system.providers.amazon.aws.example_google_api_youtube_to_s3`
============================================================================

.. py:module:: tests.system.providers.amazon.aws.example_google_api_youtube_to_s3

.. autoapi-nested-parse::

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



Module Contents
---------------


Functions
~~~~~~~~~

.. autoapisummary::

   tests.system.providers.amazon.aws.example_google_api_youtube_to_s3.create_connection_gcp
   tests.system.providers.amazon.aws.example_google_api_youtube_to_s3.wait_for_bucket
   tests.system.providers.amazon.aws.example_google_api_youtube_to_s3.transform_video_ids



Attributes
~~~~~~~~~~

.. autoapisummary::

   tests.system.providers.amazon.aws.example_google_api_youtube_to_s3.DAG_ID
   tests.system.providers.amazon.aws.example_google_api_youtube_to_s3.YOUTUBE_CHANNEL_ID
   tests.system.providers.amazon.aws.example_google_api_youtube_to_s3.YOUTUBE_VIDEO_PUBLISHED_AFTER
   tests.system.providers.amazon.aws.example_google_api_youtube_to_s3.YOUTUBE_VIDEO_PUBLISHED_BEFORE
   tests.system.providers.amazon.aws.example_google_api_youtube_to_s3.YOUTUBE_VIDEO_PARTS
   tests.system.providers.amazon.aws.example_google_api_youtube_to_s3.YOUTUBE_VIDEO_FIELDS
   tests.system.providers.amazon.aws.example_google_api_youtube_to_s3.SECRET_ARN_KEY
   tests.system.providers.amazon.aws.example_google_api_youtube_to_s3.sys_test_context_task
   tests.system.providers.amazon.aws.example_google_api_youtube_to_s3.test_context
   tests.system.providers.amazon.aws.example_google_api_youtube_to_s3.test_run


.. py:data:: DAG_ID
   :value: 'example_google_api_youtube_to_s3'



.. py:data:: YOUTUBE_CHANNEL_ID
   :value: 'UCSXwxpWZQ7XZ1WL3wqevChA'



.. py:data:: YOUTUBE_VIDEO_PUBLISHED_AFTER
   :value: '2019-09-25T00:00:00Z'



.. py:data:: YOUTUBE_VIDEO_PUBLISHED_BEFORE
   :value: '2019-10-18T00:00:00Z'



.. py:data:: YOUTUBE_VIDEO_PARTS
   :value: 'snippet'



.. py:data:: YOUTUBE_VIDEO_FIELDS
   :value: 'items(id,snippet(description,publishedAt,tags,title))'



.. py:data:: SECRET_ARN_KEY
   :value: 'SECRET_ARN'



.. py:data:: sys_test_context_task



.. py:function:: create_connection_gcp(conn_id_name, secret_arn)


.. py:function:: wait_for_bucket(s3_bucket_name)


.. py:function:: transform_video_ids(**kwargs)


.. py:data:: test_context



.. py:data:: test_run
