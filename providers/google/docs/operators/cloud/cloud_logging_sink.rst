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


Create a log sink (Dict-based)
------------------------------

You can create a Cloud Logging sink using a Python dictionary:

:class:`~airflow.providers.google.cloud.operators.cloud_logging_sink.CloudLoggingCreateSinkOperator`

.. exampleinclude:: /../../google/tests/system/google/cloud/cloud_logging_sink/example_cloud_logging_sink.py
   :language: python
   :dedent: 4
   :start-after: [START howto_operator_cloud_logging_create_sink_native_obj]
   :end-before: [END howto_operator_cloud_logging_create_sink_native_obj]

***Required fields in ``sink_config`` (dict):***

- ``name``: The name of the sink.
- ``destination``: The export destination (e.g., ``storage.googleapis.com/...``, ``bigquery.googleapis.com/...``).

Other fields such as ``description``, ``filter``, ``disabled``, and ``exclusions`` are optional.

Update a log Sink (Protobuf)
----------------------------

You can also provide the ``sink_config`` as a ``google.cloud.logging_v2.types.LogSink`` Protobuf object,
and the ``update_mask`` as a ``google.protobuf.field_mask_pb2.FieldMask``.

The following import is required when using a Protobuf object:

.. exampleinclude:: /../../google/tests/system/google/cloud/cloud_logging_sink/example_cloud_logging_sink.py
   :language: python
   :start-after: [START howto_operator_import_protobuf_obj]
   :end-before: [END howto_operator_import_protobuf_obj]



:class:`~airflow.providers.google.cloud.operators.cloud_logging_sink.CloudLoggingUpdateSinkOperator`

.. exampleinclude:: /../../google/tests/system/google/cloud/cloud_logging_sink/example_cloud_logging_sink.py
   :language: python
   :dedent: 4
   :start-after: [START howto_operator_cloud_logging_update_sink_protobuf_obj]
   :end-before: [END howto_operator_cloud_logging_update_sink_protobuf_obj]

**When updating a sink**, only include the fields you want to change in ``sink_config``, and list those fields in the ``update_mask["paths"]``.

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
