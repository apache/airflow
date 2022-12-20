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

Azure Functions Operators
=========================
Azure Functions is a serverless solution that allows you to write less code, maintain less infrastructure,
and save on costs. Instead of worrying about deploying and maintaining servers, the cloud infrastructure provides
all the up-to-date resources needed to keep your applications running. Azure Functions allows you to implement your
system's logic into readily available blocks of code. These code blocks are called "functions".

Operators
---------

.. _howto/operator:AzureFunctionsInvokeOperator:

Invoke an Azure functions
-------------------------
Use the :class:`~airflow.providers.microsoft.azure.operators.azure_functions.AzureFunctionsInvokeOperator` to invoke the
functions in azure. Below is an example of using this operator.

  .. exampleinclude:: /../../tests/system/providers/microsoft/azure/example_azure_functions.py
      :language: python
      :dedent: 0
      :start-after: [START howto_operator_invoke_azure_functions]
      :end-before: [END howto_operator_invoke_azure_functions]


Reference
---------

For further information, please refer to the Microsoft documentation:

  * `Azure Functions <https://learn.microsoft.com/en-us/azure/azure-functions/>`__
