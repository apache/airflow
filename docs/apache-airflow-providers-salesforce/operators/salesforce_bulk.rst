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

.. _howto/operator:SalesforceBulkInsertOperator:


SalesforceBulkInsertOperator
============================

Use the :class:`~airflow.providers.salesforce.operators.salesforce_bulk.SalesforceBulkInsertOperator` to execute Bulk Insert API.

Using the Operator
^^^^^^^^^^^^^^^^^^

You can use this operator to access Bulk Insert API:

.. exampleinclude:: /../../tests/system/providers/salesforce/example_salesforce_bulk.py
    :language: python
    :start-after: [START howto_salesforce_bulk_insert_operator]
    :end-before: [END howto_salesforce_bulk_insert_operator]


.. _howto/operator:SalesforceBulkUpdateOperator:


SalesforceBulkUpdateOperator
============================

Use the :class:`~airflow.providers.salesforce.operators.salesforce_bulk.SalesforceBulkUpdateOperator` to execute Bulk Update API.

Using the Operator
^^^^^^^^^^^^^^^^^^
You can use this operator to access Bulk Update API:

.. exampleinclude:: /../../tests/system/providers/salesforce/example_salesforce_bulk.py
    :language: python
    :start-after: [START howto_salesforce_bulk_update_operator]
    :end-before: [END howto_salesforce_bulk_update_operator]


.. _howto/operator:SalesforceBulkUpsertOperator:


SalesforceBulkUpsertOperator
============================

Use the :class:`~airflow.providers.salesforce.operators.salesforce_bulk.SalesforceBulkUpsertOperator` to execute Bulk Upsert API.

Using the Operator
^^^^^^^^^^^^^^^^^^
You can use this operator to access Bulk Upsert API:

.. exampleinclude:: /../../tests/system/providers/salesforce/example_salesforce_bulk.py
    :language: python
    :start-after: [START howto_salesforce_bulk_upsert_operator]
    :end-before: [END howto_salesforce_bulk_upsert_operator]


.. _howto/operator:SalesforceBulkDeleteOperator:


SalesforceBulkDeleteOperator
============================

Use the :class:`~airflow.providers.salesforce.operators.salesforce_bulk.SalesforceBulkDeleteOperator` to execute Bulk Delete API.

Using the Operator
^^^^^^^^^^^^^^^^^^
You can use this operator to access Bulk Delete API:

.. exampleinclude:: /../../tests/system/providers/salesforce/example_salesforce_bulk.py
    :language: python
    :start-after: [START howto_salesforce_bulk_delete_operator]
    :end-before: [END howto_salesforce_bulk_delete_operator]
