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



Google Cloud Financial Services Operators
=========================================

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include:: /operators/_partials/prerequisite_tasks.rst

Manage instances
^^^^^^^^^^^^^^^

.. _howto/operator:FinancialServicesCreateInstanceOperator:

Create an AML AI instance
-------------------------

Use the :class:`~airflow.providers.google.cloud.operators.financial_services.FinancialServicesCreateInstanceOperator`
operator to create an AML AI instance in the Google Cloud Financial Services API.

Using the operator
""""""""""""""""""

.. exampleinclude:: /../../providers/tests/system/google/cloud/financial_services/example_financial_services.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_financial_services_create_instance]
    :end-before: [END howto_operator_financial_services_create_instance]

Templating
""""""""""

.. literalinclude:: /../../providers/src/airflow/providers/google/cloud/operators/financial_services.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_financial_services_create_instance_template_fields]
    :end-before: [END howto_operator_financial_services_create_instance_template_fields]

More information
""""""""""""""""

See Google Cloud Financial Services API documentation to `create an instance
<https://cloud.google.com/financial-services/anti-money-laundering/docs/reference/rest/v1/projects.locations.instances/create>`_.

.. _howto/operator:FinancialServicesCreateInstanceOperator:


.. _howto/operator:FinancialServicesGetInstanceOperator:

Get an AML AI instance
-------------------------

Use the :class:`~airflow.providers.google.cloud.operators.financial_services.FinancialServicesGetInstanceOperator`
operator to get an AML AI instance in the Google Cloud Financial Services API.

Using the operator
""""""""""""""""""

.. exampleinclude:: /../../providers/tests/system/google/cloud/financial_services/example_financial_services.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_financial_services_get_instance]
    :end-before: [END howto_operator_financial_services_get_instance]

Templating
""""""""""

.. literalinclude:: /../../providers/src/airflow/providers/google/cloud/operators/financial_services.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_financial_services_get_instance_template_fields]
    :end-before: [END howto_operator_financial_services_get_instance_template_fields]

More information
""""""""""""""""

See Google Cloud Financial Services API documentation to `get an instance
<https://cloud.google.com/financial-services/anti-money-laundering/docs/reference/rest/v1/projects.locations.instances/get>`_.

.. _howto/operator:FinancialServicesGetInstanceOperator:


.. _howto/operator:FinancialServicesDeleteInstanceOperator:

Get an AML AI instance
-------------------------

Use the :class:`~airflow.providers.google.cloud.operators.financial_services.FinancialServicesDeleteInstanceOperator`
operator to delete an AML AI instance in the Google Cloud Financial Services API.

Using the operator
""""""""""""""""""

.. exampleinclude:: /../../providers/tests/system/google/cloud/financial_services/example_financial_services.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_financial_services_delete_instance]
    :end-before: [END howto_operator_financial_services_delete_instance]

Templating
""""""""""

.. literalinclude:: /../../providers/src/airflow/providers/google/cloud/operators/financial_services.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_financial_services_delete_instance_template_fields]
    :end-before: [END howto_operator_financial_services_delete_instance_template_fields]

More information
""""""""""""""""

See Google Cloud Financial Services API documentation to `delete an instance
<https://cloud.google.com/financial-services/anti-money-laundering/docs/reference/rest/v1/projects.locations.instances/delete>`_.

.. _howto/operator:FinancialServicesDeleteInstanceOperator:
