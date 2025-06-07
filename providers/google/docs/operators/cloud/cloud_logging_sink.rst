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



Google Cloud Logging Sink Operators
===================================

Cloud Logging allows you to export log entries outside of Cloud Logging using sinks.

For more information, visit `Cloud Logging documentation <https://cloud.google.com/logging/docs/export>`__.

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include:: /operators/_partials/prerequisite_tasks.rst

Create a log sink
-----------------

You can create a Cloud Logging sink using:

:class:`~airflow.providers.google.cloud.operators.cloud_logging_sink.CloudLoggingCreateSinkOperator`

.. exampleinclude:: /../../google/tests/system/google/cloud/cloud_logging_sink/example_cloud_logging_sink.py
   :language: python
   :dedent: 4
   :start-after: [START howto_operator_cloud_logging_create_sink]
   :end-before: [END howto_operator_cloud_logging_create_sink]

Update a log sink
-----------------

To update an existing sink's configuration (destination, filter, exclusions, etc.), use:

:class:`~airflow.providers.google.cloud.operators.cloud_logging_sink.CloudLoggingUpdateSinkOperator`

.. exampleinclude:: /../../google/tests/system/google/cloud/cloud_logging_sink/example_cloud_logging_sink.py
   :language: python
   :dedent: 4
   :start-after: [START howto_operator_cloud_logging_update_sink]
   :end-before: [END howto_operator_cloud_logging_update_sink]

List log sinks
--------------

To list all sinks in a Google Cloud project:

:class:`~airflow.providers.google.cloud.operators.cloud_logging_sink.CloudLoggingListSinksOperator`

.. exampleinclude:: /../../google/tests/system/google/cloud/cloud_logging_sink/example_cloud_logging_sink.py
   :language: python
   :dedent: 4
   :start-after: [START howto_operator_cloud_logging_list_sinks]
   :end-before: [END howto_operator_cloud_logging_list_sinks]

This operator returns a list of sink dictionaries from the project.



Delete a log sink
-----------------

To delete a sink from a Google Cloud project:

:class:`~airflow.providers.google.cloud.operators.cloud_logging_sink.CloudLoggingDeleteSinkOperator`

.. exampleinclude:: /../../google/tests/system/google/cloud/cloud_logging_sink/example_cloud_logging_sink.py
   :language: python
   :dedent: 4
   :start-after: [START howto_operator_cloud_logging_delete_sink]
   :end-before: [END howto_operator_cloud_logging_delete_sink]
