
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

Google API to Amazon S3 Transfer Operator
=========================================

Use the GoogleApiToS3Transfer transfer to call requests to any Google API which supports discovery and save its response
on Amazon S3.

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include:: ../_partials/prerequisite_tasks.rst

.. _howto/operator:GoogleApiToS3Transfer:

Google Sheets to Amazon S3
^^^^^^^^^^^^^^^^^^^^^^^^^^

This example loads data from Google Sheets and save it to an Amazon S3 file.

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_google_api_sheets_to_s3.py
    :language: python
    :dedent: 4
    :start-after: [START howto_transfer_google_api_sheets_to_s3]
    :end-before: [END howto_transfer_google_api_sheets_to_s3]

You can find more information about the Google API endpoint used
`here <https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values/get>`__.

Google Youtube to Amazon S3
^^^^^^^^^^^^^^^^^^^^^^^^^^^

This is a more advanced example dag for using ``GoogleApiToS3Transfer`` which uses xcom to pass data between
tasks to retrieve specific information about YouTube videos.

Get YouTube Videos
""""""""""""""""""

It searches for up to 50 videos (due to pagination) in a given time range
(``YOUTUBE_VIDEO_PUBLISHED_AFTER``, ``YOUTUBE_VIDEO_PUBLISHED_BEFORE``) on a YouTube channel (``YOUTUBE_CHANNEL_ID``)
saves the response in Amazon S3 and also pushes the data to xcom.

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_google_api_youtube_to_s3.py
    :language: python
    :dedent: 4
    :start-after: [START howto_transfer_google_api_youtube_search_to_s3]
    :end-before: [END howto_transfer_google_api_youtube_search_to_s3]

It passes over the YouTube IDs to the next request which then gets the
information (``YOUTUBE_VIDEO_FIELDS``) for the requested videos and saves them in Amazon S3 (``S3_BUCKET_NAME``).

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_google_api_youtube_to_s3.py
    :language: python
    :dedent: 4
    :start-after: [START howto_transfer_google_api_youtube_list_to_s3]
    :end-before: [END howto_transfer_google_api_youtube_list_to_s3]

Reference
^^^^^^^^^

For further information, look at:

* `Google API Client library <https://github.com/googleapis/google-api-python-client>`__
* `Google Sheets API v4 Documentation <https://developers.google.com/sheets/api/guides/concepts>`__
* `YouTube Data API v3 Documentation <https://developers.google.com/youtube/v3/docs>`__
* `AWS boto3 Library Documentation for S3 <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html>`__
