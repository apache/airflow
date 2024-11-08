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

Google Cloud Datastore Operators
================================

Firestore in Datastore mode is a NoSQL document database built for automatic scaling,
high performance, and ease of application development.

For more information about the service visit
`Datastore product documentation <https://cloud.google.com/datastore/docs>`__

Prerequisite Tasks
------------------

.. include:: /operators/_partials/prerequisite_tasks.rst


.. _howto/operator:CloudDatastoreExportEntitiesOperator:

Export Entities
---------------

To export entities from Google Cloud Datastore to Cloud Storage use
:class:`~airflow.providers.google.cloud.operators.datastore.CloudDatastoreExportEntitiesOperator`

.. exampleinclude:: /../../providers/tests/system/google/cloud/datastore/example_datastore_commit.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_export_task]
    :end-before: [END how_to_export_task]

.. _howto/operator:CloudDatastoreImportEntitiesOperator:

Import Entities
---------------

To import entities from Cloud Storage to Google Cloud Datastore use
:class:`~airflow.providers.google.cloud.operators.datastore.CloudDatastoreImportEntitiesOperator`

.. exampleinclude:: /../../providers/tests/system/google/cloud/datastore/example_datastore_commit.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_import_task]
    :end-before: [END how_to_import_task]

.. _howto/operator:CloudDatastoreAllocateIdsOperator:

Allocate Ids
------------

To allocate IDs for incomplete keys use
:class:`~airflow.providers.google.cloud.operators.datastore.CloudDatastoreAllocateIdsOperator`

.. exampleinclude:: /../../providers/tests/system/google/cloud/datastore/example_datastore_commit.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_allocate_ids]
    :end-before: [END how_to_allocate_ids]

An example of a partial keys required by the operator:

.. exampleinclude:: /../../providers/tests/system/google/cloud/datastore/example_datastore_commit.py
    :language: python
    :dedent: 0
    :start-after: [START how_to_keys_def]
    :end-before: [END how_to_keys_def]

.. _howto/operator:CloudDatastoreBeginTransactionOperator:

Begin transaction
-----------------

To begin a new transaction use
:class:`~airflow.providers.google.cloud.operators.datastore.CloudDatastoreBeginTransactionOperator`

.. exampleinclude:: /../../providers/tests/system/google/cloud/datastore/example_datastore_commit.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_begin_transaction]
    :end-before: [END how_to_begin_transaction]

An example of a transaction options required by the operator:

.. exampleinclude:: /../../providers/tests/system/google/cloud/datastore/example_datastore_commit.py
    :language: python
    :dedent: 0
    :start-after: [START how_to_transaction_def]
    :end-before: [END how_to_transaction_def]

.. _howto/operator:CloudDatastoreCommitOperator:

Commit transaction
------------------

To commit a transaction, optionally creating, deleting or modifying some entities
use :class:`~airflow.providers.google.cloud.operators.datastore.CloudDatastoreCommitOperator`

.. exampleinclude:: /../../providers/tests/system/google/cloud/datastore/example_datastore_commit.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_commit_task]
    :end-before: [END how_to_commit_task]

An example of a commit information required by the operator:

.. exampleinclude:: /../../providers/tests/system/google/cloud/datastore/example_datastore_commit.py
    :language: python
    :dedent: 0
    :start-after: [START how_to_commit_def]
    :end-before: [END how_to_commit_def]

.. _howto/operator:CloudDatastoreRunQueryOperator:

Run query
---------

To run a query for entities use
:class:`~airflow.providers.google.cloud.operators.datastore.CloudDatastoreRunQueryOperator`

.. exampleinclude:: /../../providers/tests/system/google/cloud/datastore/example_datastore_query.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_run_query]
    :end-before: [END how_to_run_query]

An example of a query required by the operator:

.. exampleinclude:: /../../providers/tests/system/google/cloud/datastore/example_datastore_query.py
    :language: python
    :dedent: 0
    :start-after: [START how_to_query_def]
    :end-before: [END how_to_query_def]

.. _howto/operator:CloudDatastoreRollbackOperator:

Roll back transaction
---------------------

To roll back a transaction
use :class:`~airflow.providers.google.cloud.operators.datastore.CloudDatastoreRollbackOperator`

.. exampleinclude:: /../../providers/tests/system/google/cloud/datastore/example_datastore_rollback.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_rollback_transaction]
    :end-before: [END how_to_rollback_transaction]

.. _howto/operator:CloudDatastoreGetOperationOperator:

Get operation state
-------------------

To get the current state of a long-running operation use
:class:`~airflow.providers.google.cloud.operators.datastore.CloudDatastoreGetOperationOperator`

.. exampleinclude:: /../../providers/tests/system/google/cloud/datastore/example_datastore_commit.py
    :language: python
    :dedent: 4
    :start-after: [START get_operation_state]
    :end-before: [END get_operation_state]

.. _howto/operator:CloudDatastoreDeleteOperationOperator:

Delete operation
----------------

To delete an operation use
:class:`~airflow.providers.google.cloud.operators.datastore.CloudDatastoreDeleteOperationOperator`

.. exampleinclude:: /../../providers/tests/system/google/cloud/datastore/example_datastore_commit.py
    :language: python
    :dedent: 4
    :start-after: [START delete_operation]
    :end-before: [END delete_operation]

References
^^^^^^^^^^
For further information, take a look at:

* `Datastore API documentation <https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects>`__
* `Product documentation <https://cloud.google.com/datastore/docs>`__
