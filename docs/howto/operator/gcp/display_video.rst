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

Google Display & Video 360 Operators
=======================================
`Google Display & Video 360 <https://marketingplatform.google.com/about/display-video-360/>`__ has the end-to-end
campaign management features you need.

.. contents::
  :depth: 1
  :local:

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include:: _partials/prerequisite_tasks.rst

.. _howto/operator:GoogleDisplayVideo360CreateReportOperator:

Creating a report
^^^^^^^^^^^^^^^^^

To create Display&Video 360 report use
:class:`~airflow.providers.google.marketing_platform.operators.display_video.GoogleDisplayVideo360CreateReportOperator`.

.. exampleinclude:: ../../../../airflow/providers/google/marketing_platform/example_dags/example_display_video.py
    :language: python
    :dedent: 4
    :start-after: [START howto_google_display_video_createquery_report_operator]
    :end-before: [END howto_google_display_video_createquery_report_operator]

Use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.marketing_platform.operators.display_video.GoogleDisplayVideo360CreateReportOperator`
parameters which allow you to dynamically determine values.
The result is saved to :ref:`XCom <concepts:xcom>`, which allows the result to be used by other operators.

.. _howto/operator:GoogleDisplayVideo360DeleteReportOperator:

Deleting a report
^^^^^^^^^^^^^^^^^

To delete Display&Video 360 report use
:class:`~airflow.providers.google.marketing_platform.operators.display_video.GoogleDisplayVideo360DeleteReportOperator`.

.. exampleinclude:: ../../../../airflow/providers/google/marketing_platform/example_dags/example_display_video.py
    :language: python
    :dedent: 4
    :start-after: [START howto_google_display_video_deletequery_report_operator]
    :end-before: [END howto_google_display_video_deletequery_report_operator]

You can use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.marketing_platform.operators.display_video.GoogleDisplayVideo360DeleteReportOperator`
parameters which allow you to dynamically determine values.

.. _howto/operator:GoogleDisplayVideo360ReportSensor:

Waiting for report
^^^^^^^^^^^^^^^^^^
To delete Display&Video 360 report use
:class:`~airflow.providers.google.marketing_platform.sensors.display_video.GoogleDisplayVideo360ReportSensor`.

.. exampleinclude:: ../../../../airflow/providers/google/marketing_platform/example_dags/example_display_video.py
    :language: python
    :dedent: 4
    :start-after: [START howto_google_display_video_wait_report_operator]
    :end-before: [END howto_google_display_video_wait_report_operator]

Use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.marketing_platform.sensors.display_video.GoogleDisplayVideo360ReportSensor`
parameters which allow you to dynamically determine values.

.. _howto/operator:GoogleDisplayVideo360DownloadReportOperator:

Downloading a report
^^^^^^^^^^^^^^^^^^^^

To download a report to GCS bucket use
:class:`~airflow.providers.google.marketing_platform.operators.display_video.GoogleDisplayVideo360DownloadReportOperator`.

.. exampleinclude:: ../../../../airflow/providers/google/marketing_platform/example_dags/example_display_video.py
    :language: python
    :dedent: 4
    :start-after: [START howto_google_display_video_getquery_report_operator]
    :end-before: [END howto_google_display_video_getquery_report_operator]

Use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.marketing_platform.operators.display_video.GoogleDisplayVideo360DownloadReportOperator`
parameters which allow you to dynamically determine values.


.. _howto/operator:GoogleDisplayVideo360RunReportOperator:

Running a report
^^^^^^^^^^^^^^^^

To run Display&Video 360 report use
:class:`~airflow.providers.google.marketing_platform.operators.display_video.GoogleDisplayVideo360RunReportOperator`.

.. exampleinclude:: ../../../../airflow/providers/google/marketing_platform/example_dags/example_display_video.py
    :language: python
    :dedent: 4
    :start-after: [START howto_google_display_video_runquery_report_operator]
    :end-before: [END howto_google_display_video_runquery_report_operator]

Use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.marketing_platform.operators.display_video.GoogleDisplayVideo360RunReportOperator`
parameters which allow you to dynamically determine values.


.. _howto/operator:GoogleDisplayVideo360DownloadLineItemsOperator:

Downloading Line Items
^^^^^^^^^^^^^^^^^^^^^^

The operator accepts body request:

- consistent with `Google API <https://developers.google.com/bid-manager/v1.1/lineitems/downloadlineitems>`_ ::

    REQUEST_BODY = {
    "filterType": ADVERTISER_ID,
    "format": "CSV",
    "fileSpec": "EWF"
    }

To download line items in CSV format report use
:class:`~airflow.providers.google.marketing_platform.operators.display_video.GoogleDisplayVideo360DownloadLineItemsOperator`.

.. exampleinclude:: ../../../../airflow/providers/google/marketing_platform/example_dags/example_display_video.py
    :language: python
    :dedent: 4
    :start-after: [START howto_google_display_video_download_line_items_operator]
    :end-before: [END howto_google_display_video_download_line_items_operator]

Use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.marketing_platform.operators.display_video.GoogleDisplayVideo360DownloadLineItemsOperator`
parameters which allow you to dynamically determine values.


.. _howto/operator:GoogleDisplayVideo360UploadLineItemsOperator:

Upload line items
^^^^^^^^^^^^^^^^^

To run Display&Video 360 uploading line items use
:class:`~airflow.providers.google.marketing_platform.operators.display_video.GoogleDisplayVideo360UploadLineItemsOperator`.

.. exampleinclude:: ../../../../airflow/providers/google/marketing_platform/example_dags/example_display_video.py
    :language: python
    :dedent: 4
    :start-after: [START howto_google_display_video_upload_line_items_operator]
    :end-before: [END howto_google_display_video_upload_line_items_operator]

Use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.marketing_platform.operators.display_video.GoogleDisplayVideo360UploadLineItemsOperator`
parameters which allow you to dynamically determine values.
