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


Azure Batch Operator
=================================

AzureBatchOperator
----------------------------------
Use the
:class:`~airflow.providers.microsoft.azure.operators.batch.AzureBatchOperator` to trigger a task on Azure Batch

Below is an example of using this operator to trigger a task on Azure Batch

.. exampleinclude:: /../../providers/tests/system/microsoft/azure/example_azure_batch_operator.py
    :language: python
    :dedent: 0
    :start-after: [START howto_azure_batch_operator]
    :end-before: [END howto_azure_batch_operator]


Reference
---------

For further information, look at:

* `Azure Batch Documentation <https://azure.microsoft.com/en-us/products/batch/>`__
