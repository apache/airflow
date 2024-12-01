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


Azure DataLake Storage Operators
=================================

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include:: /operators/_partials/prerequisite_tasks.rst

.. _howto/operator:ADLSCreateObjectOperator:

ADLSCreateObjectOperator
----------------------------------

:class:`~airflow.providers.microsoft.azure.operators.adls.ADLSCreateObjectOperator` allows you to
upload data to Azure DataLake Storage


Below is an example of using this operator to upload data to ADL.

.. exampleinclude:: /../../providers/tests/system/microsoft/azure/example_adls_create.py
    :language: python
    :dedent: 0
    :start-after: [START howto_operator_adls_create]
    :end-before: [END howto_operator_adls_create]

.. _howto/operator:ADLSDeleteOperator:

ADLSDeleteOperator
----------------------------------
Use the
:class:`~airflow.providers.microsoft.azure.operators.adls.ADLSDeleteOperator` to remove
file(s) from Azure DataLake Storage


Below is an example of using this operator to delete a file from ADL.

.. exampleinclude:: /../../providers/tests/system/microsoft/azure/example_adls_delete.py
    :language: python
    :dedent: 0
    :start-after: [START howto_operator_adls_delete]
    :end-before: [END howto_operator_adls_delete]

.. _howto/operator:ADLSListOperator:

ADLSListOperator
----------------------------------
Use the
:class:`~airflow.providers.microsoft.azure.operators.adls.ADLSListOperator` to list all
file(s) from Azure DataLake Storage


Below is an example of using this operator to list files from ADL.

.. exampleinclude:: /../../providers/tests/system/microsoft/azure/example_adls_list.py
    :language: python
    :dedent: 0
    :start-after: [START howto_operator_adls_list]
    :end-before: [END howto_operator_adls_list]


Reference
---------

For further information, look at:

* `Azure Data lake Storage Documentation <https://docs.microsoft.com/en-us/azure/data-lake-store/>`__
