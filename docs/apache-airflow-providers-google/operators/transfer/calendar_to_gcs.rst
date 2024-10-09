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

Google Calendar to Google Cloud Storage Transfer Operators
==========================================================

Google has a service `Google Cloud Storage <https://cloud.google.com/storage/>`__. This service is
used to store large data from various applications.

With `Google Calendar <https://www.google.com/calendar/about/>`__, you can quickly schedule
meetings and events and get reminders about upcoming activities, so you always know what's next.

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include:: /operators/_partials/prerequisite_tasks.rst

.. _howto/operator:GoogleCalendarToGCSOperator:

Upload data from Google Calendar to GCS
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To upload data from Google Calendar to Google Cloud Storage you can use the
:class:`~airflow.providers.google.cloud.transfers.calendar_to_gcs.GoogleCalendarToGCSOperator`.

.. exampleinclude:: /../../providers/tests/system/google/cloud/gcs/example_calendar_to_gcs.py
    :language: python
    :dedent: 4
    :start-after: [START upload_calendar_to_gcs]
    :end-before: [END upload_calendar_to_gcs]

You can use :ref:`Jinja templating <concepts:jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.transfers.calendar_to_gcs.GoogleCalendarToGCSOperator`.
