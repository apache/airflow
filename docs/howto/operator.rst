..  Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

..    http://www.apache.org/licenses/LICENSE-2.0

..  Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

Using Operators
===============

An operator represents a single, ideally idempotent, task. Operators
determine what actually executes when your DAG runs.

See the :ref:`Operators Concepts <concepts-operators>` documentation and the
:ref:`Operators API Reference <api-reference-operators>` for more
information.

.. contents:: :local:

.. _howto/operator:BashOperator:

BashOperator
------------

Use the :class:`~airflow.operators.bash_operator.BashOperator` to execute
commands in a `Bash <https://www.gnu.org/software/bash/>`__ shell.

.. literalinclude:: ../../airflow/example_dags/example_bash_operator.py
    :language: python
    :start-after: [START howto_operator_bash]
    :end-before: [END howto_operator_bash]

Templating
^^^^^^^^^^

You can use :ref:`Jinja templates <jinja-templating>` to parameterize the
``bash_command`` argument.

.. literalinclude:: ../../airflow/example_dags/example_bash_operator.py
    :language: python
    :start-after: [START howto_operator_bash_template]
    :end-before: [END howto_operator_bash_template]

Troubleshooting
^^^^^^^^^^^^^^^

Jinja template not found
""""""""""""""""""""""""

Add a space after the script name when directly calling a Bash script with
the ``bash_command`` argument. This is because Airflow tries to apply a Jinja
template to it, which will fail.

.. code-block:: python

    t2 = BashOperator(
        task_id='bash_example',

        # This fails with `Jinja template not found` error
        # bash_command="/home/batcher/test.sh",

        # This works (has a space after)
        bash_command="/home/batcher/test.sh ",
        dag=dag)

.. _howto/operator:PythonOperator:

PythonOperator
--------------

Use the :class:`~airflow.operators.python_operator.PythonOperator` to execute
Python callables.

.. literalinclude:: ../../airflow/example_dags/example_python_operator.py
    :language: python
    :start-after: [START howto_operator_python]
    :end-before: [END howto_operator_python]

Passing in arguments
^^^^^^^^^^^^^^^^^^^^

Use the ``op_args`` and ``op_kwargs`` arguments to pass additional arguments
to the Python callable.

.. literalinclude:: ../../airflow/example_dags/example_python_operator.py
    :language: python
    :start-after: [START howto_operator_python_kwargs]
    :end-before: [END howto_operator_python_kwargs]

Templating
^^^^^^^^^^

When you set the ``provide_context`` argument to ``True``, Airflow passes in
an additional set of keyword arguments: one for each of the :ref:`Jinja
template variables <macros>` and a ``templates_dict`` argument.

The ``templates_dict`` argument is templated, so each value in the dictionary
is evaluated as a :ref:`Jinja template <jinja-templating>`.

Google Cloud Storage Operators
------------------------------

.. _howto/operator:GoogleCloudStorageToBigQueryOperator:

GoogleCloudStorageToBigQueryOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Use the
:class:`~airflow.contrib.operators.gcs_to_bq.GoogleCloudStorageToBigQueryOperator`
to execute a BigQuery load job.

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcs_to_bq_operator.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcs_to_bq]
    :end-before: [END howto_operator_gcs_to_bq]


Google Compute Engine Operators
-------------------------------

.. _howto/operator:GceInstanceStartOperator:

GceInstanceStartOperator
^^^^^^^^^^^^^^^^^^^^^^^^

Use the
:class:`~airflow.contrib.operators.gcp_compute_operator.GceInstanceStartOperator`
to start an existing Google Compute Engine instance.


Arguments
"""""""""

The following examples of OS environment variables used to pass arguments to the operator:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_compute.py
    :language: python
    :start-after: [START howto_operator_gce_args_common]
    :end-before: [END howto_operator_gce_args_common]

Using the operator
""""""""""""""""""

The code to create the operator:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_compute.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gce_start]
    :end-before: [END howto_operator_gce_start]

You can also create the operator without project id - project id will be retrieved
from the GCP connection id used:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_compute.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gce_start_no_project_id]
    :end-before: [END howto_operator_gce_start_no_project_id]


Templating
""""""""""

.. literalinclude:: ../../airflow/contrib/operators/gcp_compute_operator.py
    :language: python
    :dedent: 4
    :start-after: [START gce_instance_start_template_fields]
    :end-before: [END gce_instance_start_template_fields]

More information
""""""""""""""""

See `Google Compute Engine API documentation for start
<https://cloud.google.com/compute/docs/reference/rest/v1/instances/start>`_.

.. _howto/operator:GceInstanceStopOperator:

GceInstanceStopOperator
^^^^^^^^^^^^^^^^^^^^^^^

Use the operator to stop Google Compute Engine instance.

For parameter definition, take a look at
:class:`~airflow.contrib.operators.gcp_compute_operator.GceInstanceStopOperator`

Arguments
"""""""""

The following examples of OS environment variables used to pass arguments to the operator:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_compute.py
   :language: python
   :start-after: [START howto_operator_gce_args_common]
   :end-before: [END howto_operator_gce_args_common]

Using the operator
""""""""""""""""""

The code to create the operator:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_compute.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gce_stop]
    :end-before: [END howto_operator_gce_stop]

You can also create the operator without project id - project id will be retrieved
from the GCP connection used:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_compute.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gce_stop_no_project_id]
    :end-before: [END howto_operator_gce_stop_no_project_id]

Templating
""""""""""

.. literalinclude:: ../../airflow/contrib/operators/gcp_compute_operator.py
    :language: python
    :dedent: 4
    :start-after: [START gce_instance_stop_template_fields]
    :end-before: [END gce_instance_stop_template_fields]

More information
""""""""""""""""

See `Google Compute Engine API documentation for stop
<https://cloud.google.com/compute/docs/reference/rest/v1/instances/stop>`_.

.. _howto/operator:GceSetMachineTypeOperator:

GceSetMachineTypeOperator
^^^^^^^^^^^^^^^^^^^^^^^^^

Use the operator to change machine type of a Google Compute Engine instance.

For parameter definition, take a look at
:class:`~airflow.contrib.operators.gcp_compute_operator.GceSetMachineTypeOperator`.

Arguments
"""""""""

The following examples of OS environment variables used to pass arguments to the operator:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_compute.py
    :language: python
    :start-after: [START howto_operator_gce_args_common]
    :end-before: [END howto_operator_gce_args_common]


.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_compute.py
    :language: python
    :start-after: [START howto_operator_gce_args_set_machine_type]
    :end-before: [END howto_operator_gce_args_set_machine_type]

Using the operator
""""""""""""""""""

The code to create the operator:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_compute.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gce_set_machine_type]
    :end-before: [END howto_operator_gce_set_machine_type]

You can also create the operator without project id - project id will be retrieved
from the GCP connection used:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_compute.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gce_set_machine_type_no_project_id]
    :end-before: [END howto_operator_gce_set_machine_type_no_project_id]

Templating
""""""""""

.. literalinclude:: ../../airflow/contrib/operators/gcp_compute_operator.py
    :language: python
    :dedent: 4
    :start-after: [START gce_instance_set_machine_type_template_fields]
    :end-before: [END gce_instance_set_machine_type_template_fields]

More information
""""""""""""""""

See `Google Compute Engine API documentation for setMachineType
<https://cloud.google.com/compute/docs/reference/rest/v1/instances/setMachineType>`_.

.. _howto/operator:GceInstanceTemplateCopyOperator:

GceInstanceTemplateCopyOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Use the operator to copy an existing Google Compute Engine instance template
applying a patch to it.

For parameter definition, take a look at
:class:`~airflow.contrib.operators.gcp_compute_operator.GceInstanceTemplateCopyOperator`.

Arguments
"""""""""

The following examples of OS environment variables used to pass arguments to the operator:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_compute_igm.py
    :language: python
    :start-after: [START howto_operator_compute_igm_common_args]
    :end-before: [END howto_operator_compute_igm_common_args]

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_compute_igm.py
    :language: python
    :start-after: [START howto_operator_compute_template_copy_args]
    :end-before: [END howto_operator_compute_template_copy_args]

Using the operator
""""""""""""""""""

The code to create the operator:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_compute_igm.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gce_igm_copy_template]
    :end-before: [END howto_operator_gce_igm_copy_template]

You can also create the operator without project id - project id will be retrieved
from the GCP connection used:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_compute_igm.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gce_igm_copy_template_no_project_id]
    :end-before: [END howto_operator_gce_igm_copy_template_no_project_id]

Templating
""""""""""

.. literalinclude:: ../../airflow/contrib/operators/gcp_compute_operator.py
    :language: python
    :dedent: 4
    :start-after: [START gce_instance_template_copy_operator_template_fields]
    :end-before: [END gce_instance_template_copy_operator_template_fields]

More information
""""""""""""""""

See `Google Compute Engine API documentation for instanceTemplates
<https://cloud.google.com/compute/docs/reference/rest/v1/instanceTemplates>`_.

.. _howto/operator:GceInstanceGroupManagerUpdateTemplateOperator:

GceInstanceGroupManagerUpdateTemplateOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Use the operator to update template in Google Compute Engine Instance Group Manager.

For parameter definition, take a look at
:class:`~airflow.contrib.operators.gcp_compute_operator.GceInstanceGroupManagerUpdateTemplateOperator`.

Arguments
"""""""""

The following examples of OS environment variables used to pass arguments to the operator:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_compute_igm.py
    :language: python
    :start-after: [START howto_operator_compute_igm_common_args]
    :end-before: [END howto_operator_compute_igm_common_args]

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_compute_igm.py
    :language: python
    :start-after: [START howto_operator_compute_igm_update_template_args]
    :end-before: [END howto_operator_compute_igm_update_template_args]

Using the operator
""""""""""""""""""

The code to create the operator:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_compute_igm.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gce_igm_update_template]
    :end-before: [END howto_operator_gce_igm_update_template]

You can also create the operator without project id - project id will be retrieved
from the GCP connection used:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_compute_igm.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gce_igm_update_template_no_project_id]
    :end-before: [END howto_operator_gce_igm_update_template_no_project_id]


Templating
""""""""""

.. literalinclude:: ../../airflow/contrib/operators/gcp_compute_operator.py
    :language: python
    :dedent: 4
    :start-after: [START gce_igm_update_template_operator_template_fields]
    :end-before: [END gce_igm_update_template_operator_template_fields]

Troubleshooting
"""""""""""""""

You might find that your GceInstanceGroupManagerUpdateTemplateOperator fails with
missing permissions. To execute the operation, the service account requires
the permissions that theService Account User role provides
(assigned via Google Cloud IAM).

More information
""""""""""""""""

See `Google Compute Engine API documentation for instanceGroupManagers
<https://cloud.google.com/compute/docs/reference/rest/v1/instanceGroupManagers>`_.

Google Cloud Bigtable Operators
-------------------------------

Arguments
^^^^^^^^^

All examples below rely on the following variables, which can be passed via environment variables.

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_bigtable_operators.py
    :language: python
    :start-after: [START howto_operator_gcp_bigtable_args]
    :end-before: [END howto_operator_gcp_bigtable_args]

.. _howto/operator:BigtableInstanceCreateOperator:

BigtableInstanceCreateOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Use the :class:`~airflow.contrib.operators.gcp_bigtable_operator.BigtableInstanceCreateOperator`
to create a Google Cloud Bigtable instance.

If the Cloud Bigtable instance with the given ID exists, the operator does not compare its configuration
and immediately succeeds. No changes are made to the existing instance.

Using the operator
""""""""""""""""""

You can create the operator with or without project id. If project id is missing
it will be retrieved from the GCP connection used. Both variants are shown:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_bigtable_operators.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_bigtable_instance_create]
    :end-before: [END howto_operator_gcp_bigtable_instance_create]

.. _howto/operator:BigtableInstanceDeleteOperator:

BigtableInstanceDeleteOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Use the :class:`~airflow.contrib.operators.gcp_bigtable_operator.BigtableInstanceDeleteOperator`
to delete a Google Cloud Bigtable instance.

Using the operator
""""""""""""""""""

You can create the operator with or without project id. If project id is missing
it will be retrieved from the GCP connection used. Both variants are shown:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_bigtable_operators.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_bigtable_instance_delete]
    :end-before: [END howto_operator_gcp_bigtable_instance_delete]

.. _howto/operator:BigtableClusterUpdateOperator:

BigtableClusterUpdateOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Use the :class:`~airflow.contrib.operators.gcp_bigtable_operator.BigtableClusterUpdateOperator`
to modify number of nodes in a Cloud Bigtable cluster.

Using the operator
""""""""""""""""""

You can create the operator with or without project id. If project id is missing
it will be retrieved from the GCP connection used. Both variants are shown:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_bigtable_operators.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_bigtable_cluster_update]
    :end-before: [END howto_operator_gcp_bigtable_cluster_update]

.. _howto/operator:BigtableTableCreateOperator:

BigtableTableCreateOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Creates a table in a Cloud Bigtable instance.

If the table with given ID exists in the Cloud Bigtable instance, the operator compares the Column Families.
If the Column Families are identical operator succeeds. Otherwise, the operator fails with the appropriate
error message.


Using the operator
""""""""""""""""""

You can create the operator with or without project id. If project id is missing
it will be retrieved from the GCP connection used. Both variants are shown:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_bigtable_operators.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_bigtable_table_create]
    :end-before: [END howto_operator_gcp_bigtable_table_create]

Advanced
""""""""

When creating a table, you can specify the optional ``initial_split_keys`` and ``column_familes``.
Please refer to the Python Client for Google Cloud Bigtable documentation
`for Table <https://googleapis.github.io/google-cloud-python/latest/bigtable/table.html>`_ and `for Column
Families <https://googleapis.github.io/google-cloud-python/latest/bigtable/column-family.html>`_.

.. _howto/operator:BigtableTableDeleteOperator:

BigtableTableDeleteOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Use the :class:`~airflow.contrib.operators.gcp_bigtable_operator.BigtableTableDeleteOperator`
to delete a table in Google Cloud Bigtable.

Using the operator
""""""""""""""""""

You can create the operator with or without project id. If project id is missing
it will be retrieved from the GCP connection used. Both variants are shown:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_bigtable_operators.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_bigtable_table_delete]
    :end-before: [END howto_operator_gcp_bigtable_table_delete]

.. _howto/operator:BigtableTableWaitForReplicationSensor:

BigtableTableWaitForReplicationSensor
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can create the operator with or without project id. If project id is missing
it will be retrieved from the GCP connection used. Both variants are shown:

Use the :class:`~airflow.contrib.operators.gcp_bigtable_operator.BigtableTableWaitForReplicationSensor`
to wait for the table to replicate fully.

The same arguments apply to this sensor as the BigtableTableCreateOperator_.

**Note:** If the table or the Cloud Bigtable instance does not exist, this sensor waits for the table until
timeout hits and does not raise any exception.

Using the operator
""""""""""""""""""

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_bigtable_operators.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_bigtable_table_wait_for_replication]
    :end-before: [END howto_operator_gcp_bigtable_table_wait_for_replication]

Google Cloud Functions Operators
--------------------------------

.. _howto/operator:GcfFunctionDeleteOperator:

GcfFunctionDeleteOperator
^^^^^^^^^^^^^^^^^^^^^^^^^

Use the operator to delete a function from Google Cloud Functions.

For parameter definition, take a look at
:class:`~airflow.contrib.operators.gcp_function_operator.GcfFunctionDeleteOperator`.

Arguments
"""""""""

The following examples of OS environment variables show how you can build function name
to use in the operator:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_function.py
    :language: python
    :start-after: [START howto_operator_gcf_common_variables]
    :end-before: [END howto_operator_gcf_common_variables]

Using the operator
""""""""""""""""""

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_function.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcf_delete]
    :end-before: [END howto_operator_gcf_delete]

Templating
""""""""""

.. literalinclude:: ../../airflow/contrib/operators/gcp_function_operator.py
    :language: python
    :dedent: 4
    :start-after: [START gcf_function_delete_template_fields]
    :end-before: [END gcf_function_delete_template_fields]

More information
""""""""""""""""

See `Google Cloud Functions API documentation for delete
<https://cloud.google.com/functions/docs/reference/rest/v1/projects.locations.functions/delete>`_.

.. _howto/operator:GcfFunctionDeployOperator:

GcfFunctionDeployOperator
^^^^^^^^^^^^^^^^^^^^^^^^^

Use the operator to deploy a function to Google Cloud Functions.
If a function with this name already exists, it will be updated.

For parameter definition, take a look at
:class:`~airflow.contrib.operators.gcp_function_operator.GcfFunctionDeployOperator`.


Arguments
"""""""""

In the example DAG the following environment variables are used to parameterize the
operator's definition:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_function.py
    :language: python
    :start-after: [START howto_operator_gcf_common_variables]
    :end-before: [END howto_operator_gcf_common_variables]

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_function.py
    :language: python
    :start-after: [START howto_operator_gcf_deploy_variables]
    :end-before: [END howto_operator_gcf_deploy_variables]

Some of those variables are used to create the request's body:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_function.py
    :language: python
    :start-after: [START howto_operator_gcf_deploy_body]
    :end-before: [END howto_operator_gcf_deploy_body]

When a DAG is created, the default_args dictionary can be used to pass
arguments common with other tasks:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_function.py
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

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_function.py
    :language: python
    :start-after: [START howto_operator_gcf_deploy_variants]
    :end-before: [END howto_operator_gcf_deploy_variants]

The code to create the operator:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_function.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcf_deploy]
    :end-before: [END howto_operator_gcf_deploy]

You can also create the operator without project id - project id will be retrieved
from the GCP connection used:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_function.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcf_deploy_no_project_id]
    :end-before: [END howto_operator_gcf_deploy_no_project_id]

Templating
""""""""""

.. literalinclude:: ../../airflow/contrib/operators/gcp_function_operator.py
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

The typical way of assigning Cloud IAM permissions with `gcloud` is
shown below. Just replace PROJECT_ID with ID of your Google Cloud Platform project
and SERVICE_ACCOUNT_EMAIL with the email ID of your service account.

.. code-block:: bash

  gcloud iam service-accounts add-iam-policy-binding \
    PROJECT_ID@appspot.gserviceaccount.com \
    --member="serviceAccount:[SERVICE_ACCOUNT_EMAIL]" \
    --role="roles/iam.serviceAccountUser"

You can also do that via the GCP Web console.

See `Adding the IAM service agent user role to the runtime service <https://cloud.google.com/functions/docs/reference/iam/roles#adding_the_iam_service_agent_user_role_to_the_runtime_service_account>`_  for details.

If the source code for your function is in Google Source Repository, make sure that
your service account has the Source Repository Viewer role so that the source code
can be downloaded if necessary.

More information
""""""""""""""""

See `Google Cloud Functions API documentation for create
<https://cloud.google.com/functions/docs/reference/rest/v1/projects.locations.functions/create>`_.

Google Cloud Spanner Operators
------------------------------

.. _howto/operator:CloudSpannerInstanceDatabaseDeleteOperator:

CloudSpannerInstanceDatabaseDeleteOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Deletes a database from the specified Cloud Spanner instance. If the database does not
exist, no action is taken, and the operator succeeds.

For parameter definition, take a look at
:class:`~airflow.contrib.operators.gcp_spanner_operator.CloudSpannerInstanceDatabaseDeleteOperator`.

Arguments
"""""""""

Some arguments in the example DAG are taken from environment variables.

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_spanner.py
    :language: python
    :start-after: [START howto_operator_spanner_arguments]
    :end-before: [END howto_operator_spanner_arguments]

Using the operator
""""""""""""""""""

You can create the operator with or without project id. If project id is missing
it will be retrieved from the GCP connection used. Both variants are shown:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_spanner.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_spanner_database_delete]
    :end-before: [END howto_operator_spanner_database_delete]

Templating
""""""""""

.. literalinclude:: ../../airflow/contrib/operators/gcp_spanner_operator.py
    :language: python
    :dedent: 4
    :start-after: [START gcp_spanner_delete_template_fields]
    :end-before: [END gcp_spanner_delete_template_fields]

More information
""""""""""""""""

See `Google Cloud Spanner API documentation for database drop call
<https://cloud.google.com/spanner/docs/reference/rest/v1/projects.instances.databases/dropDatabase>`_.

.. _howto/operator:CloudSpannerInstanceDatabaseDeployOperator:

CloudSpannerInstanceDatabaseDeployOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Creates a new Cloud Spanner database in the specified instance, or if the
desired database exists, assumes success with no changes applied to database
configuration. No structure of the database is verified - it's enough if the database exists
with the same name.

For parameter definition, take a look at
:class:`~airflow.contrib.operators.gcp_spanner_operator.CloudSpannerInstanceDatabaseDeployOperator`.

Arguments
"""""""""

Some arguments in the example DAG are taken from environment variables.

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_spanner.py
    :language: python
    :start-after: [START howto_operator_spanner_arguments]
    :end-before: [END howto_operator_spanner_arguments]

Using the operator
""""""""""""""""""

You can create the operator with or without project id. If project id is missing
it will be retrieved from the GCP connection used. Both variants are shown:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_spanner.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_spanner_database_deploy]
    :end-before: [END howto_operator_spanner_database_deploy]

Templating
""""""""""

.. literalinclude:: ../../airflow/contrib/operators/gcp_spanner_operator.py
    :language: python
    :dedent: 4
    :start-after: [START gcp_spanner_database_deploy_template_fields]
    :end-before: [END gcp_spanner_database_deploy_template_fields]

More information
""""""""""""""""

See Google Cloud Spanner API documentation for `database create
<https://cloud.google.com/spanner/docs/reference/rest/v1/projects.instances.databases/create>`_

.. _howto/operator:CloudSpannerInstanceDatabaseUpdateOperator:

CloudSpannerInstanceDatabaseUpdateOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Runs a DDL query in a Cloud Spanner database and allows you to modify the structure of an
existing database.

You can optionally specify an operation_id parameter which simplifies determining whether
the statements were executed in case the update_database call is replayed
(idempotency check). The operation_id should be unique within the database, and must be
a valid identifier: `[a-z][a-z0-9_]*`. More information can be found in
`the documentation of updateDdl API <https://cloud.google.com/spanner/docs/reference/rest/v1/projects.instances.databases/updateDdl>`_

For parameter definition take a look at
:class:`~airflow.contrib.operators.gcp_spanner_operator.CloudSpannerInstanceDatabaseUpdateOperator`.

Arguments
"""""""""

Some arguments in the example DAG are taken from environment variables.

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_spanner.py
    :language: python
    :start-after: [START howto_operator_spanner_arguments]
    :end-before: [END howto_operator_spanner_arguments]

Using the operator
""""""""""""""""""

You can create the operator with or without project id. If project id is missing
it will be retrieved from the GCP connection used. Both variants are shown:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_spanner.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_spanner_database_update]
    :end-before: [END howto_operator_spanner_database_update]

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_spanner.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_spanner_database_update_idempotent]
    :end-before: [END howto_operator_spanner_database_update_idempotent]

Templating
""""""""""

.. literalinclude:: ../../airflow/contrib/operators/gcp_spanner_operator.py
    :language: python
    :dedent: 4
    :start-after: [START gcp_spanner_database_update_template_fields]
    :end-before: [END gcp_spanner_database_update_template_fields]

More information
""""""""""""""""

See Google Cloud Spanner API documentation for `database update_ddl
<https://cloud.google.com/spanner/docs/reference/rest/v1/projects.instances.databases/updateDdl>`_.

.. _howto/operator:CloudSpannerInstanceDatabaseQueryOperator:

CloudSpannerInstanceDatabaseQueryOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Executes an arbitrary DML query (INSERT, UPDATE, DELETE).

For parameter definition take a look at
:class:`~airflow.contrib.operators.gcp_spanner_operator.CloudSpannerInstanceDatabaseQueryOperator`.

Arguments
"""""""""

Some arguments in the example DAG are taken from environment variables.

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_spanner.py
    :language: python
    :start-after: [START howto_operator_spanner_arguments]
    :end-before: [END howto_operator_spanner_arguments]

Using the operator
""""""""""""""""""

You can create the operator with or without project id. If project id is missing
it will be retrieved from the GCP connection used. Both variants are shown:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_spanner.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_spanner_query]
    :end-before: [END howto_operator_spanner_query]

Templating
""""""""""

.. literalinclude:: ../../airflow/contrib/operators/gcp_spanner_operator.py
    :language: python
    :dedent: 4
    :start-after: [START gcp_spanner_query_template_fields]
    :end-before: [END gcp_spanner_query_template_fields]

More information
""""""""""""""""

See Google Cloud Spanner API documentation for `the DML syntax
<https://cloud.google.com/spanner/docs/dml-syntax>`_.

.. _howto/operator:CloudSpannerInstanceDeleteOperator:

CloudSpannerInstanceDeleteOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Deletes a Cloud Spanner instance. If an instance does not exist, no action is taken,
and the operator succeeds.

For parameter definition take a look at
:class:`~airflow.contrib.operators.gcp_spanner_operator.CloudSpannerInstanceDeleteOperator`.

Arguments
"""""""""

Some arguments in the example DAG are taken from environment variables:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_spanner.py
    :language: python
    :start-after: [START howto_operator_spanner_arguments]
    :end-before: [END howto_operator_spanner_arguments]

Using the operator
""""""""""""""""""

You can create the operator with or without project id. If project id is missing
it will be retrieved from the GCP connection used. Both variants are shown:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_spanner.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_spanner_delete]
    :end-before: [END howto_operator_spanner_delete]

Templating
""""""""""

.. literalinclude:: ../../airflow/contrib/operators/gcp_spanner_operator.py
  :language: python
  :dedent: 4
  :start-after: [START gcp_spanner_delete_template_fields]
  :end-before: [END gcp_spanner_delete_template_fields]

More information
""""""""""""""""

See `Google Cloud Spanner API documentation for instance delete
<https://cloud.google.com/spanner/docs/reference/rest/v1/projects.instances/delete>`_.

Google Cloud Sql Operators
--------------------------

.. _howto/operator:CloudSqlInstanceDatabaseCreateOperator:

CloudSqlInstanceDatabaseCreateOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Creates a new database inside a Cloud SQL instance.

For parameter definition, take a look at
:class:`~airflow.contrib.operators.gcp_sql_operator.CloudSqlInstanceDatabaseCreateOperator`.

Arguments
"""""""""

Some arguments in the example DAG are taken from environment variables:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_sql.py
    :language: python
    :start-after: [START howto_operator_cloudsql_arguments]
    :end-before: [END howto_operator_cloudsql_arguments]

Using the operator
""""""""""""""""""

You can create the operator with or without project id. If project id is missing
it will be retrieved from the GCP connection used. Both variants are shown:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_sql.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_cloudsql_db_create]
    :end-before: [END howto_operator_cloudsql_db_create]

Example request body:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_sql.py
    :language: python
    :start-after: [START howto_operator_cloudsql_db_create_body]
    :end-before: [END howto_operator_cloudsql_db_create_body]

Templating
""""""""""

.. literalinclude:: ../../airflow/contrib/operators/gcp_sql_operator.py
  :language: python
  :dedent: 4
  :start-after: [START gcp_sql_db_create_template_fields]
  :end-before: [END gcp_sql_db_create_template_fields]

More information
""""""""""""""""

See `Google Cloud SQL API documentation for database insert
<https://cloud.google.com/sql/docs/mysql/admin-api/v1beta4/databases/insert>`_.

.. _howto/operator:CloudSqlInstanceDatabaseDeleteOperator:

CloudSqlInstanceDatabaseDeleteOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Deletes a database from a Cloud SQL instance.

For parameter definition, take a look at
:class:`~airflow.contrib.operators.gcp_sql_operator.CloudSqlInstanceDatabaseDeleteOperator`.

Arguments
"""""""""

Some arguments in the example DAG are taken from environment variables:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_sql.py
    :language: python
    :start-after: [START howto_operator_cloudsql_arguments]
    :end-before: [END howto_operator_cloudsql_arguments]

Using the operator
""""""""""""""""""

You can create the operator with or without project id. If project id is missing
it will be retrieved from the GCP connection used. Both variants are shown:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_sql.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_cloudsql_db_delete]
    :end-before: [END howto_operator_cloudsql_db_delete]

Templating
""""""""""

.. literalinclude:: ../../airflow/contrib/operators/gcp_sql_operator.py
  :language: python
  :dedent: 4
  :start-after: [START gcp_sql_db_delete_template_fields]
  :end-before: [END gcp_sql_db_delete_template_fields]

More information
""""""""""""""""

See `Google Cloud SQL API documentation for database delete
<https://cloud.google.com/sql/docs/mysql/admin-api/v1beta4/databases/delete>`_.

.. _howto/operator:CloudSqlInstanceDatabasePatchOperator:

CloudSqlInstanceDatabasePatchOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Updates a resource containing information about a database inside a Cloud SQL instance
using patch semantics.
See: https://cloud.google.com/sql/docs/mysql/admin-api/how-tos/performance#patch

For parameter definition, take a look at
:class:`~airflow.contrib.operators.gcp_sql_operator.CloudSqlInstanceDatabasePatchOperator`.

Arguments
"""""""""

Some arguments in the example DAG are taken from environment variables:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_sql.py
    :language: python
    :start-after: [START howto_operator_cloudsql_arguments]
    :end-before: [END howto_operator_cloudsql_arguments]

Using the operator
""""""""""""""""""

You can create the operator with or without project id. If project id is missing
it will be retrieved from the GCP connection used. Both variants are shown:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_sql.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_cloudsql_db_patch]
    :end-before: [END howto_operator_cloudsql_db_patch]

Example request body:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_sql.py
    :language: python
    :start-after: [START howto_operator_cloudsql_db_patch_body]
    :end-before: [END howto_operator_cloudsql_db_patch_body]

Templating
""""""""""

.. literalinclude:: ../../airflow/contrib/operators/gcp_sql_operator.py
  :language: python
  :dedent: 4
  :start-after: [START gcp_sql_db_patch_template_fields]
  :end-before: [END gcp_sql_db_patch_template_fields]

More information
""""""""""""""""

See `Google Cloud SQL API documentation for database patch
<https://cloud.google.com/sql/docs/mysql/admin-api/v1beta4/databases/patch>`_.

.. _howto/operator:CloudSqlInstanceDeleteOperator:

CloudSqlInstanceDeleteOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Deletes a Cloud SQL instance in Google Cloud Platform.

It is also used for deleting read and failover replicas.

For parameter definition, take a look at
:class:`~airflow.contrib.operators.gcp_sql_operator.CloudSqlInstanceDeleteOperator`.

Arguments
"""""""""

Some arguments in the example DAG are taken from OS environment variables:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_sql.py
    :language: python
    :start-after: [START howto_operator_cloudsql_arguments]
    :end-before: [END howto_operator_cloudsql_arguments]

Using the operator
""""""""""""""""""

You can create the operator with or without project id. If project id is missing
it will be retrieved from the GCP connection used. Both variants are shown:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_sql.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_cloudsql_delete]
    :end-before: [END howto_operator_cloudsql_delete]

Note: If the instance has read or failover replicas you need to delete them before you delete the primary instance.
Replicas are deleted the same way as primary instances:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_sql.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_cloudsql_replicas_delete]
    :end-before: [END howto_operator_cloudsql_replicas_delete]

Templating
""""""""""

.. literalinclude:: ../../airflow/contrib/operators/gcp_sql_operator.py
  :language: python
  :dedent: 4
  :start-after: [START gcp_sql_delete_template_fields]
  :end-before: [END gcp_sql_delete_template_fields]

More information
""""""""""""""""

See `Google Cloud SQL API documentation for delete
<https://cloud.google.com/sql/docs/mysql/admin-api/v1beta4/instances/delete>`_.

.. _howto/operator:CloudSqlInstanceExportOperator:

CloudSqlInstanceExportOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Exports data from a Cloud SQL instance to a Cloud Storage bucket as a SQL dump
or CSV file.

.. note::
    This operator is idempotent. If executed multiple times with the same
    export file URI, the export file in GCS will simply be overridden.

For parameter definition take a look at
:class:`~airflow.contrib.operators.gcp_sql_operator.CloudSqlInstanceExportOperator`.

Arguments
"""""""""

Some arguments in the example DAG are taken from Airflow variables:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_sql.py
    :language: python
    :start-after: [START howto_operator_cloudsql_arguments]
    :end-before: [END howto_operator_cloudsql_arguments]

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_sql.py
    :language: python
    :start-after: [START howto_operator_cloudsql_export_import_arguments]
    :end-before: [END howto_operator_cloudsql_export_import_arguments]

Example body defining the export operation:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_sql.py
    :language: python
    :start-after: [START howto_operator_cloudsql_export_body]
    :end-before: [END howto_operator_cloudsql_export_body]

Using the operator
""""""""""""""""""

You can create the operator with or without project id. If project id is missing
it will be retrieved from the GCP connection used. Both variants are shown:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_sql.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_cloudsql_export]
    :end-before: [END howto_operator_cloudsql_export]

Templating
""""""""""

.. literalinclude:: ../../airflow/contrib/operators/gcp_sql_operator.py
    :language: python
    :dedent: 4
    :start-after: [START gcp_sql_export_template_fields]
    :end-before: [END gcp_sql_export_template_fields]

More information
""""""""""""""""

See `Google Cloud SQL API documentation for export <https://cloud.google
.com/sql/docs/mysql/admin-api/v1beta4/instances/export>`_.

Troubleshooting
"""""""""""""""

If you receive an "Unauthorized" error in GCP, make sure that the service account
of the Cloud SQL instance is authorized to write to the selected GCS bucket.

It is not the service account configured in Airflow that communicates with GCS,
but rather the service account of the particular Cloud SQL instance.

To grant the service account with the appropriate WRITE permissions for the GCS bucket
you can use the :class:`~airflow.contrib.operators.gcs_acl_operator.GoogleCloudStorageBucketCreateAclEntryOperator`,
as shown in the example:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_sql.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_cloudsql_export_gcs_permissions]
    :end-before: [END howto_operator_cloudsql_export_gcs_permissions]


.. _howto/operator:CloudSqlInstanceImportOperator:

CloudSqlInstanceImportOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Imports data into a Cloud SQL instance from a SQL dump or CSV file in Cloud Storage.

CSV import:
"""""""""""

This operator is NOT idempotent for a CSV import. If the same file is imported
multiple times, the imported data will be duplicated in the database.
Moreover, if there are any unique constraints the duplicate import may result in an
error.

SQL import:
"""""""""""

This operator is idempotent for a SQL import if it was also exported by Cloud SQL.
The exported SQL contains 'DROP TABLE IF EXISTS' statements for all tables
to be imported.

If the import file was generated in a different way, idempotence is not guaranteed.
It has to be ensured on the SQL file level.

For parameter definition take a look at
:class:`~airflow.contrib.operators.gcp_sql_operator.CloudSqlInstanceImportOperator`.

Arguments
"""""""""

Some arguments in the example DAG are taken from Airflow variables:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_sql.py
    :language: python
    :start-after: [START howto_operator_cloudsql_arguments]
    :end-before: [END howto_operator_cloudsql_arguments]

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_sql.py
    :language: python
    :start-after: [START howto_operator_cloudsql_export_import_arguments]
    :end-before: [END howto_operator_cloudsql_export_import_arguments]

Example body defining the import operation:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_sql.py
    :language: python
    :start-after: [START howto_operator_cloudsql_import_body]
    :end-before: [END howto_operator_cloudsql_import_body]

Using the operator
""""""""""""""""""

You can create the operator with or without project id. If project id is missing
it will be retrieved from the GCP connection used. Both variants are shown:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_sql.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_cloudsql_import]
    :end-before: [END howto_operator_cloudsql_import]

Templating
""""""""""

.. literalinclude:: ../../airflow/contrib/operators/gcp_sql_operator.py
    :language: python
    :dedent: 4
    :start-after: [START gcp_sql_import_template_fields]
    :end-before: [END gcp_sql_import_template_fields]

More information
""""""""""""""""

See `Google Cloud SQL API documentation for import <https://cloud.google.com/sql/docs/mysql/admin-api/v1beta4/instances/import>`_.

Troubleshooting
"""""""""""""""

If you receive an "Unauthorized" error in GCP, make sure that the service account
of the Cloud SQL instance is authorized to read from the selected GCS object.

It is not the service account configured in Airflow that communicates with GCS,
but rather the service account of the particular Cloud SQL instance.

To grant the service account with the appropriate READ permissions for the GCS object
you can use the :class:`~airflow.contrib.operators.gcs_acl_operator.GoogleCloudStorageObjectCreateAclEntryOperator`,
as shown in the example:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_sql.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_cloudsql_import_gcs_permissions]
    :end-before: [END howto_operator_cloudsql_import_gcs_permissions]

.. _howto/operator:CloudSqlInstanceCreateOperator:

CloudSqlInstanceCreateOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Creates a new Cloud SQL instance in Google Cloud Platform.

It is also used for creating read replicas.

For parameter definition, take a look at
:class:`~airflow.contrib.operators.gcp_sql_operator.CloudSqlInstanceCreateOperator`.

If an instance with the same name exists, no action will be taken and the operator
will succeed.

Arguments
"""""""""

Some arguments in the example DAG are taken from OS environment variables:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_sql.py
    :language: python
    :start-after: [START howto_operator_cloudsql_arguments]
    :end-before: [END howto_operator_cloudsql_arguments]

Some other arguments are created based on the arguments above:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_sql.py
    :language: python
    :start-after: [START howto_operator_cloudsql_create_arguments]
    :end-before: [END howto_operator_cloudsql_create_arguments]

Example body defining the instance with failover replica:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_sql.py
    :language: python
    :start-after: [START howto_operator_cloudsql_create_body]
    :end-before: [END howto_operator_cloudsql_create_body]

Example body defining read replica for the instance above:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_sql.py
    :language: python
    :start-after: [START howto_operator_cloudsql_create_replica]
    :end-before: [END howto_operator_cloudsql_create_replica]

Note: Failover replicas are created together with the instance in a single task.
Read replicas need to be created in separate tasks.

Using the operator
""""""""""""""""""

You can create the operator with or without project id. If project id is missing
it will be retrieved from the GCP connection used. Both variants are shown:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_sql.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_cloudsql_create]
    :end-before: [END howto_operator_cloudsql_create]

Templating
""""""""""

.. literalinclude:: ../../airflow/contrib/operators/gcp_sql_operator.py
  :language: python
  :dedent: 4
  :start-after: [START gcp_sql_create_template_fields]
  :end-before: [END gcp_sql_create_template_fields]

More information
""""""""""""""""

See `Google Cloud SQL API documentation for insert
<https://cloud.google.com/sql/docs/mysql/admin-api/v1beta4/instances/insert>`_.

.. _howto/operator:CloudSqlInstancePatchOperator:

CloudSqlInstancePatchOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Updates settings of a Cloud SQL instance in Google Cloud Platform (partial update).

For parameter definition, take a look at
:class:`~airflow.contrib.operators.gcp_sql_operator.CloudSqlInstancePatchOperator`.

This is a partial update, so only values for the settings specified in the body
will be set / updated. The rest of the existing instance's configuration will remain
unchanged.

Arguments
"""""""""

Some arguments in the example DAG are taken from OS environment variables:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_sql.py
    :language: python
    :start-after: [START howto_operator_cloudsql_arguments]
    :end-before: [END howto_operator_cloudsql_arguments]

Example body defining the instance:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_sql.py
    :language: python
    :start-after: [START howto_operator_cloudsql_patch_body]
    :end-before: [END howto_operator_cloudsql_patch_body]

Using the operator
""""""""""""""""""

You can create the operator with or without project id. If project id is missing
it will be retrieved from the GCP connection used. Both variants are shown:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_sql.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_cloudsql_patch]
    :end-before: [END howto_operator_cloudsql_patch]

Templating
""""""""""

.. literalinclude:: ../../airflow/contrib/operators/gcp_sql_operator.py
  :language: python
  :dedent: 4
  :start-after: [START gcp_sql_patch_template_fields]
  :end-before: [END gcp_sql_patch_template_fields]

More information
""""""""""""""""

See `Google Cloud SQL API documentation for patch
<https://cloud.google.com/sql/docs/mysql/admin-api/v1beta4/instances/patch>`_.

.. _howto/operator:CloudSqlQueryOperator:

CloudSqlQueryOperator
^^^^^^^^^^^^^^^^^^^^^

Performs DDL or DML SQL queries in Google Cloud SQL instance. The DQL
(retrieving data from Google Cloud SQL) is not supported. You might run the SELECT
queries, but the results of those queries are discarded.

You can specify various connectivity methods to connect to running instance,
starting from public IP plain connection through public IP with SSL or both TCP and
socket connection via Cloud SQL Proxy. The proxy is downloaded and started/stopped
dynamically as needed by the operator.

There is a *gcpcloudsql://* connection type that you should use to define what
kind of connectivity you want the operator to use. The connection is a "meta"
type of connection. It is not used to make an actual connectivity on its own, but it
determines whether Cloud SQL Proxy should be started by `CloudSqlDatabaseHook`
and what kind of database connection (Postgres or MySQL) should be created
dynamically to connect to Cloud SQL via public IP address or via the proxy.
The 'CloudSqlDatabaseHook` uses
:class:`~airflow.contrib.hooks.gcp_sql_hook.CloudSqlProxyRunner` to manage Cloud SQL
Proxy lifecycle (each task has its own Cloud SQL Proxy)

When you build connection, you should use connection parameters as described in
:class:`~airflow.contrib.hooks.gcp_sql_hook.CloudSqlDatabaseHook`. You can see
examples of connections below for all the possible types of connectivity. Such connection
can be reused between different tasks (instances of `CloudSqlQueryOperator`). Each
task will get their own proxy started if needed with their own TCP or UNIX socket.

For parameter definition, take a look at
:class:`~airflow.contrib.operators.gcp_sql_operator.CloudSqlQueryOperator`.

Since query operator can run arbitrary query, it cannot be guaranteed to be
idempotent. SQL query designer should design the queries to be idempotent. For example,
both Postgres and MySQL support CREATE TABLE IF NOT EXISTS statements that can be
used to create tables in an idempotent way.

Arguments
"""""""""

If you define connection via `AIRFLOW_CONN_*` URL defined in an environment
variable, make sure the URL components in the URL are URL-encoded.
See examples below for details.

Note that in case of SSL connections you need to have a mechanism to make the
certificate/key files available in predefined locations for all the workers on
which the operator can run. This can be provided for example by mounting
NFS-like volumes in the same path for all the workers.

Some arguments in the example DAG are taken from the OS environment variables:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_sql_query.py
      :language: python
      :start-after: [START howto_operator_cloudsql_query_arguments]
      :end-before: [END howto_operator_cloudsql_query_arguments]

Example connection definitions for all connectivity cases. Note that all the components
of the connection URI should be URL-encoded:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_sql_query.py
      :language: python
      :start-after: [START howto_operator_cloudsql_query_connections]
      :end-before: [END howto_operator_cloudsql_query_connections]

Using the operator
""""""""""""""""""

Example operators below are using all connectivity options. Note connection id
from the operator matches the `AIRFLOW_CONN_*` postfix uppercase. This is
standard AIRFLOW notation for defining connection via environment variables):

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_sql_query.py
      :language: python
      :start-after: [START howto_operator_cloudsql_query_operators]
      :end-before: [END howto_operator_cloudsql_query_operators]

Templating
""""""""""

.. literalinclude:: ../../airflow/contrib/operators/gcp_sql_operator.py
    :language: python
    :dedent: 4
    :start-after: [START gcp_sql_query_template_fields]
    :end-before: [END gcp_sql_query_template_fields]

More information
""""""""""""""""

See `Google Cloud SQL Proxy documentation
<https://cloud.google.com/sql/docs/postgres/sql-proxy>`_.

Google Cloud Storage Operators
------------------------------

.. _howto/operator:GoogleCloudStorageBucketCreateAclEntryOperator:

GoogleCloudStorageBucketCreateAclEntryOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Creates a new ACL entry on the specified bucket.

For parameter definition, take a look at
:class:`~airflow.contrib.operators.gcs_acl_operator.GoogleCloudStorageBucketCreateAclEntryOperator`

Arguments
"""""""""

Some arguments in the example DAG are taken from the OS environment variables:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcs_acl.py
      :language: python
      :start-after: [START howto_operator_gcs_acl_args_common]
      :end-before: [END howto_operator_gcs_acl_args_common]

Using the operator
""""""""""""""""""

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcs_acl.py
      :language: python
      :dedent: 4
      :start-after: [START howto_operator_gcs_bucket_create_acl_entry_task]
      :end-before: [END howto_operator_gcs_bucket_create_acl_entry_task]

Templating
""""""""""

.. literalinclude:: ../../airflow/contrib/operators/gcs_acl_operator.py
    :language: python
    :dedent: 4
    :start-after: [START gcs_bucket_create_acl_template_fields]
    :end-before: [END gcs_bucket_create_acl_template_fields]

More information
""""""""""""""""

See `Google Cloud Storage BucketAccessControls insert documentation
<https://cloud.google.com/storage/docs/json_api/v1/bucketAccessControls/insert>`_.

.. _howto/operator:GoogleCloudStorageObjectCreateAclEntryOperator:

GoogleCloudStorageObjectCreateAclEntryOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Creates a new ACL entry on the specified object.

For parameter definition, take a look at
:class:`~airflow.contrib.operators.gcs_acl_operator.GoogleCloudStorageObjectCreateAclEntryOperator`

Arguments
"""""""""

Some arguments in the example DAG are taken from the OS environment variables:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcs_acl.py
      :language: python
      :start-after: [START howto_operator_gcs_acl_args_common]
      :end-before: [END howto_operator_gcs_acl_args_common]

Using the operator
""""""""""""""""""

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcs_acl.py
      :language: python
      :dedent: 4
      :start-after: [START howto_operator_gcs_object_create_acl_entry_task]
      :end-before: [END howto_operator_gcs_object_create_acl_entry_task]

Templating
""""""""""

.. literalinclude:: ../../airflow/contrib/operators/gcs_acl_operator.py
    :language: python
    :dedent: 4
    :start-after: [START gcs_object_create_acl_template_fields]
    :end-before: [END gcs_object_create_acl_template_fields]

More information
""""""""""""""""

See `Google Cloud Storage ObjectAccessControls insert documentation
<https://cloud.google.com/storage/docs/json_api/v1/objectAccessControls/insert>`_.
