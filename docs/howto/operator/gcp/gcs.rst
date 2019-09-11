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



Google Cloud Storage Operators
==============================

.. contents::
  :depth: 1
  :local:

.. _howto/operator:GoogleCloudStorageToBigQueryOperator:

GoogleCloudStorageToBigQueryOperator
------------------------------------

Use the
:class:`~airflow.operators.gcs_to_bq.GoogleCloudStorageToBigQueryOperator`
to execute a BigQuery load job.

.. exampleinclude:: ../../../../airflow/example_dags/example_gcs_to_bq.py
    :language: python
    :start-after: [START howto_operator_gcs_to_bq]
    :end-before: [END howto_operator_gcs_to_bq]

.. _howto/operator:GoogleCloudStorageBucketCreateAclEntryOperator:

GoogleCloudStorageBucketCreateAclEntryOperator
----------------------------------------------

Creates a new ACL entry on the specified bucket.

For parameter definition, take a look at
:class:`~airflow.contrib.operators.gcs_acl_operator.GoogleCloudStorageBucketCreateAclEntryOperator`

Arguments
"""""""""

Some arguments in the example DAG are taken from the OS environment variables:

.. exampleinclude:: ../../../../airflow/contrib/example_dags/example_gcs.py
    :language: python
    :start-after: [START howto_operator_gcs_acl_args_common]
    :end-before: [END howto_operator_gcs_acl_args_common]

Using the operator
""""""""""""""""""

.. exampleinclude:: ../../../../airflow/contrib/example_dags/example_gcs.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcs_bucket_create_acl_entry_task]
    :end-before: [END howto_operator_gcs_bucket_create_acl_entry_task]

Templating
""""""""""

.. literalinclude:: ../../../../airflow/contrib/operators/gcs_acl_operator.py
    :language: python
    :dedent: 4
    :start-after: [START gcs_bucket_create_acl_template_fields]
    :end-before: [END gcs_bucket_create_acl_template_fields]

More information
""""""""""""""""

See Google Cloud Storage Documentation to `create a new ACL entry for a bucket
<https://cloud.google.com/storage/docs/json_api/v1/bucketAccessControls/insert>`_.

.. _howto/operator:GoogleCloudStorageObjectCreateAclEntryOperator:

GoogleCloudStorageObjectCreateAclEntryOperator
----------------------------------------------

Creates a new ACL entry on the specified object.

For parameter definition, take a look at
:class:`~airflow.contrib.operators.gcs_acl_operator.GoogleCloudStorageObjectCreateAclEntryOperator`

Arguments
"""""""""

Some arguments in the example DAG are taken from the OS environment variables:

.. exampleinclude:: ../../../../airflow/contrib/example_dags/example_gcs.py
    :language: python
    :start-after: [START howto_operator_gcs_acl_args_common]
    :end-before: [END howto_operator_gcs_acl_args_common]

Using the operator
""""""""""""""""""

.. exampleinclude:: ../../../../airflow/contrib/example_dags/example_gcs.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcs_object_create_acl_entry_task]
    :end-before: [END howto_operator_gcs_object_create_acl_entry_task]

Templating
""""""""""

.. literalinclude:: ../../../../airflow/contrib/operators/gcs_acl_operator.py
    :language: python
    :dedent: 4
    :start-after: [START gcs_object_create_acl_template_fields]
    :end-before: [END gcs_object_create_acl_template_fields]

More information
""""""""""""""""

See Google Cloud Storage insert documentation to `create a ACL entry for ObjectAccess
<https://cloud.google.com/storage/docs/json_api/v1/objectAccessControls/insert>`_.

Reference
---------

For further information, look at:

* `Client Library Documentation <https://googleapis.github.io/google-cloud-python/latest/storage/index.html>`__
* `Product Documentation <https://cloud.google.com/storage/docs/>`__
