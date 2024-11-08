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

.. _howto/operator:SalesforceBulkOperator:


SalesforceBulkOperator
======================

Use the :class:`~airflow.providers.salesforce.operators.bulk.SalesforceBulkOperator` to execute Bulk API.

Using the Operator
^^^^^^^^^^^^^^^^^^

You can use this operator to access Bulk Insert API:

.. exampleinclude:: /../../providers/tests/system/salesforce/example_bulk.py
    :language: python
    :dedent: 4
    :start-after: [START howto_salesforce_bulk_insert_operation]
    :end-before: [END howto_salesforce_bulk_insert_operation]

You can use this operator to access Bulk Update API:

.. exampleinclude:: /../../providers/tests/system/salesforce/example_bulk.py
    :language: python
    :dedent: 4
    :start-after: [START howto_salesforce_bulk_update_operation]
    :end-before: [END howto_salesforce_bulk_update_operation]

You can use this operator to access Bulk Upsert API:

.. exampleinclude:: /../../providers/tests/system/salesforce/example_bulk.py
    :language: python
    :dedent: 4
    :start-after: [START howto_salesforce_bulk_upsert_operation]
    :end-before: [END howto_salesforce_bulk_upsert_operation]

You can use this operator to access Bulk Delete API:

.. exampleinclude:: /../../providers/tests/system/salesforce/example_bulk.py
    :language: python
    :dedent: 4
    :start-after: [START howto_salesforce_bulk_delete_operation]
    :end-before: [END howto_salesforce_bulk_delete_operation]
