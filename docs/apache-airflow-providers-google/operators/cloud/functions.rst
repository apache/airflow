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



Google Cloud Functions Operators
================================

.. contents::
  :depth: 1
  :local:

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include::/operators/_partials/prerequisite_tasks.rst

.. _howto/operator:CloudFunctionDeleteFunctionOperator:

CloudFunctionDeleteFunctionOperator
-----------------------------------

Use the operator to delete a function from Google Cloud Functions.

For parameter definition, take a look at
:class:`~airflow.providers.google.cloud.operators.functions.CloudFunctionDeleteFunctionOperator`.

Using the operator
""""""""""""""""""

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_functions.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcf_delete]
    :end-before: [END howto_operator_gcf_delete]

Templating
""""""""""

.. literalinclude:: /../../airflow/providers/google/cloud/operators/functions.py
    :language: python
    :dedent: 4
    :start-after: [START gcf_function_delete_template_fields]
    :end-before: [END gcf_function_delete_template_fields]

More information
""""""""""""""""

See Google Cloud Functions API documentation to `delete a function
<https://cloud.google.com/functions/docs/reference/rest/v1/projects.locations.functions/delete>`_.

.. _howto/operator:CloudFunctionDeployFunctionOperator:

CloudFunctionDeployFunctionOperator
-----------------------------------

Use the operator to deploy a function to Google Cloud Functions.
If a function with this name already exists, it will be updated.

For parameter definition, take a look at
:class:`~airflow.providers.google.cloud.operators.functions.CloudFunctionDeployFunctionOperator`.


Arguments
"""""""""

When a DAG is created, the default_args dictionary can be used to pass
arguments common with other tasks:

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_functions.py
    :language: python
    :start-after: [START howto_operator_gcf_default_args]
    :end-before: [END howto_operator_gcf_default_args]

Note that the neither the body nor the default args are complete in the above examples.
Depending on the variables set, there might be different variants on how to pass source
code related fields. Currently, you can pass either ``sourceArchiveUrl``,
``sourceRepository`` or ``sourceUploadUrl`` as described in the
`Cloud Functions API specification
<https://cloud.google.com/functions/docs/reference/rest/v1/projects.locations.functions#CloudFunction>`_.

Additionally, ``default_args`` or direct operator args might contain ``zip_path``
parameter
to run the extra step of uploading the source code before deploying it.
In this case, you also need to provide an empty ``sourceUploadUrl``
parameter in the body.

Using the operator
""""""""""""""""""

Depending on the combination of parameters, the Function's source code can be obtained
from different sources:

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_functions.py
    :language: python
    :start-after: [START howto_operator_gcf_deploy_body]
    :end-before: [END howto_operator_gcf_deploy_body]

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_functions.py
    :language: python
    :start-after: [START howto_operator_gcf_deploy_variants]
    :end-before: [END howto_operator_gcf_deploy_variants]

The code to create the operator:

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_functions.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcf_deploy]
    :end-before: [END howto_operator_gcf_deploy]

You can also create the operator without project id - project id will be retrieved
from the Google Cloud connection used:

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_functions.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcf_deploy_no_project_id]
    :end-before: [END howto_operator_gcf_deploy_no_project_id]

Templating
""""""""""

.. literalinclude:: /../../airflow/providers/google/cloud/operators/functions.py
    :language: python
    :dedent: 4
    :start-after: [START gcf_function_deploy_template_fields]
    :end-before: [END gcf_function_deploy_template_fields]


Troubleshooting
"""""""""""""""

If during the deploy you see an error similar to:

`"HttpError 403: Missing necessary permission iam.serviceAccounts.actAs for on resource
project-name@appspot.gserviceaccount.com. Please grant the
roles/iam.serviceAccountUser role."`

it means that your service account does not have the correct Cloud IAM permissions.

1. Assign your Service Account the Cloud Functions Developer role.
2. Grant the user the Cloud IAM Service Account User role on the Cloud Functions runtime
   service account.

The typical way of assigning Cloud IAM permissions with ``gcloud`` is
shown below. Just replace PROJECT_ID with ID of your Google Cloud project
and SERVICE_ACCOUNT_EMAIL with the email ID of your service account.

.. code-block:: bash

  gcloud iam service-accounts add-iam-policy-binding \
    PROJECT_ID@appspot.gserviceaccount.com \
    --member="serviceAccount:[SERVICE_ACCOUNT_EMAIL]" \
    --role="roles/iam.serviceAccountUser"

You can also do that via the Google Cloud Console.

See `Adding the IAM service agent user role to the runtime service <https://cloud.google.com/functions/docs/reference/iam/roles#adding_the_iam_service_agent_user_role_to_the_runtime_service_account>`_  for details.

If the source code for your function is in Google Source Repository, make sure that
your service account has the Source Repository Viewer role so that the source code
can be downloaded if necessary.

More information
""""""""""""""""

See Google Cloud API documentation `to create a function
<https://cloud.google.com/functions/docs/reference/rest/v1/projects.locations.functions/create>`_.


Reference
---------

For further information, look at:

* `Google Cloud API Documentation <https://cloud.google.com/functions/docs/reference/rest/>`__
* `Product Documentation <https://cloud.google.com/functions/docs/>`__
