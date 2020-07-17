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


.. _howto/operator:GoogleApiToS3Transfer:

Google API To S3 Transfer
=========================

.. contents::
  :depth: 1
  :local:

Overview
--------

The ``GoogleApiToS3Transfer`` can call requests to any Google API which supports discovery and save its response on S3.

Two example_dags are provided which showcase the
:class:`~airflow.providers.amazon.aws.transfers.google_api_to_s3.GoogleApiToS3Transfer`
in action.

 - example_google_api_to_s3_transfer_basic.py
 - example_google_api_to_s3_transfer_advanced.py

example_google_api_to_s3_transfer_basic.py
------------------------------------------

Purpose
"""""""
This is a basic example dag for using ``GoogleApiToS3Transfer`` to retrieve Google Sheets data.

Environment variables
"""""""""""""""""""""

These examples rely on the following variables, which can be passed via OS environment variables.

.. exampleinclude:: /../airflow/providers/amazon/aws/example_dags/example_google_api_to_s3_transfer_basic.py
    :language: python
    :start-after: [START howto_operator_google_api_to_s3_transfer_basic_env_variables]
    :end-before: [END howto_operator_google_api_to_s3_transfer_basic_env_variables]

All of them are required.

Get Google Sheets Sheet Values
""""""""""""""""""""""""""""""

In the following code we are requesting a Google Sheet via the ``sheets.spreadsheets.values.get`` endpoint.

.. exampleinclude:: /../airflow/providers/amazon/aws/example_dags/example_google_api_to_s3_transfer_basic.py
    :language: python
    :start-after: [START howto_operator_google_api_to_s3_transfer_basic_task_1]
    :end-before: [END howto_operator_google_api_to_s3_transfer_basic_task_1]

You can find more information to the API endpoint used
`here <https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values/get>`__.

example_google_api_to_s3_transfer_advanced.py
---------------------------------------------

Purpose
"""""""

This is a more advanced example dag for using ``GoogleApiToS3Transfer`` which uses xcom to pass data between
tasks to retrieve specific information about YouTube videos.

Environment variables
"""""""""""""""""""""

This example relies on the following variables, which can be passed via OS environment variables.

.. exampleinclude:: /../airflow/providers/amazon/aws/example_dags/example_google_api_to_s3_transfer_advanced.py
    :language: python
    :start-after: [START howto_operator_google_api_to_s3_transfer_advanced_env_variables]
    :end-before: [END howto_operator_google_api_to_s3_transfer_advanced_env_variables]

``S3_DESTINATION_KEY`` is required.

Get YouTube Videos
""""""""""""""""""

First it searches for up to 50 videos (due to pagination) in a given time range
(``YOUTUBE_VIDEO_PUBLISHED_AFTER``, ``YOUTUBE_VIDEO_PUBLISHED_BEFORE``) on a YouTube channel (``YOUTUBE_CHANNEL_ID``)
saves the response in S3 and also pushes the data to xcom.

.. exampleinclude:: /../airflow/providers/amazon/aws/example_dags/example_google_api_to_s3_transfer_advanced.py
    :language: python
    :start-after: [START howto_operator_google_api_to_s3_transfer_advanced_task_1]
    :end-before: [END howto_operator_google_api_to_s3_transfer_advanced_task_1]

From there a ``BranchPythonOperator`` will extract the xcom data and bring the IDs in a format the next
request needs it + it also decides whether we need to request any videos or not.

.. exampleinclude:: /../airflow/providers/amazon/aws/example_dags/example_google_api_to_s3_transfer_advanced.py
    :language: python
    :start-after: [START howto_operator_google_api_to_s3_transfer_advanced_task_1_2]
    :end-before: [END howto_operator_google_api_to_s3_transfer_advanced_task_1_2]

.. exampleinclude:: /../airflow/providers/amazon/aws/example_dags/example_google_api_to_s3_transfer_advanced.py
    :language: python
    :start-after: [START howto_operator_google_api_to_s3_transfer_advanced_task_1_1]
    :end-before: [END howto_operator_google_api_to_s3_transfer_advanced_task_1_1]

If there are YouTube Video IDs available, it passes over the YouTube IDs to the next request which then gets the
information (``YOUTUBE_VIDEO_FIELDS``) for the requested videos and saves them in S3 (``S3_DESTINATION_KEY``).

.. exampleinclude:: /../airflow/providers/amazon/aws/example_dags/example_google_api_to_s3_transfer_advanced.py
    :language: python
    :start-after: [START howto_operator_google_api_to_s3_transfer_advanced_task_2]
    :end-before: [END howto_operator_google_api_to_s3_transfer_advanced_task_2]

If not do nothing - and track it.

.. exampleinclude:: /../airflow/providers/amazon/aws/example_dags/example_google_api_to_s3_transfer_advanced.py
    :language: python
    :start-after: [START howto_operator_google_api_to_s3_transfer_advanced_task_2_1]
    :end-before: [END howto_operator_google_api_to_s3_transfer_advanced_task_2_1]

Reference
---------

For further information, look at:

* `Google API Client library <https://github.com/googleapis/google-api-python-client>`__
* `Google Sheets API v4 Documentation <https://developers.google.com/sheets/api/guides/concepts>`__
* `YouTube Data API v3 Documentation <https://developers.google.com/youtube/v3/docs>`__
* `AWS boto3 Library Documentation for S3 <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html>`__
