Using Operators
===============

An operator represents a single, ideally idempotent, task. Operators
determine what actually executes when your DAG runs.

See the :ref:`Operators Concepts <concepts-operators>` documentation and the
:ref:`Operators API Reference <api-reference-operators>` for more
information.

.. contents:: :local:

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

GceInstanceStartOperator
^^^^^^^^^^^^^^^^^^^^^^^^

Use the
:class:`~airflow.contrib.operators.gcp_compute_operator.GceInstanceStartOperator`
to start an existing Google Compute Engine instance.


Arguments
"""""""""

The following examples of OS environment variables show how you can build function name
to use in the operator and build default args to pass them to multiple tasks:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_compute.py
    :language: python
    :start-after: [START howto_operator_gce_args_common]
    :end-before: [END howto_operator_gce_args_common]

Using the operator
""""""""""""""""""

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_compute.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gce_start]
    :end-before: [END howto_operator_gce_start]

Templating
""""""""""

.. literalinclude:: ../../airflow/contrib/operators/gcp_compute_operator.py
    :language: python
    :dedent: 4
    :start-after: [START gce_instance_start_template_fields]
    :end-before: [END gce_instance_start_template_fields]

More information
""""""""""""""""

See `Google Compute Engine API documentation <https://cloud.google.com/compute/docs/reference/rest/v1/instances/start>`_


GceInstanceStopOperator
^^^^^^^^^^^^^^^^^^^^^^^

Use the operator to stop Google Compute Engine instance.

For parameter definition take a look at
:class:`~airflow.contrib.operators.gcp_compute_operator.GceInstanceStopOperator`

Arguments
"""""""""

The following examples of OS environment variables show how you can build function name
to use in the operator and build default args to pass them to multiple tasks:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_compute.py
   :language: python
   :start-after: [START howto_operator_gce_args_common]
   :end-before: [END howto_operator_gce_args_common]

Using the operator
""""""""""""""""""

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_compute.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gce_stop]
    :end-before: [END howto_operator_gce_stop]

Templating
""""""""""

.. literalinclude:: ../../airflow/contrib/operators/gcp_compute_operator.py
    :language: python
    :dedent: 4
    :start-after: [START gce_instance_stop_template_fields]
    :end-before: [END gce_instance_stop_template_fields]

More information
""""""""""""""""

See `Google Compute Engine API documentation <https://cloud.google.com/compute/docs/reference/rest/v1/instances/stop>`_


GceSetMachineTypeOperator
^^^^^^^^^^^^^^^^^^^^^^^^^

Use the operator to change machine type of a Google Compute Engine instance.

For parameter definition take a look at
:class:`~airflow.contrib.operators.gcp_compute_operator.GceSetMachineTypeOperator`

Arguments
"""""""""

The following examples of OS environment variables show how you can build function name
to use in the operator and build default args to pass them to multiple tasks:

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

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_compute.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gce_set_machine_type]
    :end-before: [END howto_operator_gce_set_machine_type]

Templating
""""""""""

.. literalinclude:: ../../airflow/contrib/operators/gcp_compute_operator.py
    :language: python
    :dedent: 4
    :start-after: [START gce_instance_set_machine_type_template_fields]
    :end-before: [END gce_instance_set_machine_type_template_fields]

More information
""""""""""""""""

See `Google Compute Engine API documentation <https://cloud.google.com/compute/docs/reference/rest/v1/instances/setMachineType>`_


GceInstanceTemplateCopyOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Use the operator to copy an existing Google Compute Engine instance template
applying a patch to it.

For parameter definition take a look at
:class:`~airflow.contrib.operators.gcp_compute_operator.GceInstanceTemplateCopyOperator`.

Arguments
"""""""""

The following examples of OS environment variables show how you can build parameters
passed to the operator and build default args to pass them to multiple tasks:

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

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_compute_igm.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gce_igm_copy_template]
    :end-before: [END howto_operator_gce_igm_copy_template]

Templating
""""""""""

.. literalinclude:: ../../airflow/contrib/operators/gcp_compute_operator.py
    :language: python
    :dedent: 4
    :start-after: [START gce_instance_template_copy_operator_template_fields]
    :end-before: [END gce_instance_template_copy_operator_template_fields]

More information
""""""""""""""""

See `Google Compute Engine API documentation <https://cloud.google.com/compute/docs/reference/rest/v1/instanceTemplates>`_

GceInstanceGroupManagerUpdateTemplateOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Use the operator to update template in Google Compute Engine Instance Group Manager.

For parameter definition take a look at
:class:`~airflow.contrib.operators.gcp_compute_operator.GceInstanceGroupManagerUpdateTemplateOperator`.

Arguments
"""""""""

The following examples of OS environment variables show how you can build parameters
passed to the operator and build default args to pass them to multiple tasks:

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

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_compute_igm.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gce_igm_update_template]
    :end-before: [END howto_operator_gce_igm_update_template]

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
missing permissions. The service account has to have Service Account User role assigned
via IAM permissions in order to execute the operation.

More information
""""""""""""""""

See `Google Compute Engine API documentation <https://cloud.google.com/compute/docs/reference/rest/v1/instanceGroupManagers>`_

Google Cloud Functions Operators
--------------------------------

GcfFunctionDeleteOperator
^^^^^^^^^^^^^^^^^^^^^^^^^

Use the operator to delete a function from Google Cloud Functions.

For parameter definition take a look at
:class:`~airflow.contrib.operators.gcp_function_operator.GcfFunctionDeleteOperator`.

Arguments
"""""""""

The following examples of OS environment variables show how you can build function name
to use in the operator and build default args to pass them to multiple tasks:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_function_delete.py
    :language: python
    :start-after: [START howto_operator_gcf_delete_args]
    :end-before: [END howto_operator_gcf_delete_args]

Using the operator
""""""""""""""""""

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_function_delete.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcf_delete]
    :end-before: [END howto_operator_gcf_delete]

Templating
""""""""""

.. literalinclude:: ../../airflow/contrib/operators/gcp_function_operator.py
    :language: python
    :dedent: 4
    :start-after: [START gce_function_delete_template_operator_template_fields]
    :end-before: [END gce_function_delete_template_operator_template_fields]

Troubleshooting
"""""""""""""""
If you want to run or deploy an operator using a service account and get “forbidden 403”
errors, it means that your service account does not have the correct
Cloud IAM permissions.

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


See `Adding the IAM service agent user role to the runtime service <https://cloud.google.com/functions/docs/reference/iam/roles#adding_the_iam_service_agent_user_role_to_the_runtime_service_account>`_  for details

More information
""""""""""""""""

See `Google Cloud Functions API documentation <https://cloud.google.com/functions/docs/reference/rest/v1/projects.locations.functions/delete>`_

GcfFunctionDeployOperator
^^^^^^^^^^^^^^^^^^^^^^^^^

Use the operator to deploy a function to Google Cloud Functions.

For parameter definition take a look at
:class:`~airflow.contrib.operators.gcp_function_operator.GcfFunctionDeployOperator`.


Arguments
"""""""""

The following examples of OS environment variables show various variants and combinations
of default_args that you can use. The variables are defined as follows:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_function_deploy_delete.py
    :language: python
    :start-after: [START howto_operator_gcf_deploy_variables]
    :end-before: [END howto_operator_gcf_deploy_variables]

With those variables you can define the body of the request:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_function_deploy_delete.py
    :language: python
    :start-after: [START howto_operator_gcf_deploy_body]
    :end-before: [END howto_operator_gcf_deploy_body]

When you create a DAG, the default_args dictionary can be used to pass
arguments common with other tasks:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_function_deploy_delete.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcf_deploy_args]
    :end-before: [END howto_operator_gcf_deploy_args]

Note that the neither the body nor the default args are complete in the above examples.
Depending on the set variables, there might be different variants on how to pass source
code related fields. Currently, you can pass either sourceArchiveUrl, sourceRepository
or sourceUploadUrl as described in the
`Cloud Functions API specification <https://cloud.google.com/functions/docs/reference/rest/v1/projects.locations.functions#CloudFunction>`_.
Additionally, default_args might contain zip_path parameter to run the extra step of
uploading the source code before deploying it. In the last case, you also need to
provide an empty `sourceUploadUrl` parameter in the body.

Using the operator
""""""""""""""""""

Based on the variables defined above, example logic of setting the source code
related fields is shown here:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_function_deploy_delete.py
    :language: python
    :start-after: [START howto_operator_gcf_deploy_variants]
    :end-before: [END howto_operator_gcf_deploy_variants]

The code to create the operator:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_function_deploy_delete.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcf_deploy]
    :end-before: [END howto_operator_gcf_deploy]

Templating
""""""""""

.. literalinclude:: ../../airflow/contrib/operators/gcp_function_operator.py
    :language: python
    :dedent: 4
    :start-after: [START gce_function_deploy_template_operator_template_fields]
    :end-before: [END gce_function_deploy_template_operator_template_fields]


Troubleshooting
"""""""""""""""

If you want to run or deploy an operator using a service account and get “forbidden 403”
errors, it means that your service account does not have the correct
Cloud IAM permissions.

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

See `Adding the IAM service agent user role to the runtime service <https://cloud.google.com/functions/docs/reference/iam/roles#adding_the_iam_service_agent_user_role_to_the_runtime_service_account>`_  for details

If the source code for your function is in Google Source Repository, make sure that
your service account has the Source Repository Viewer role so that the source code
can be downloaded if necessary.

More information
""""""""""""""""

See `Google Cloud Functions API documentation <https://cloud.google.com/functions/docs/reference/rest/v1/projects.locations.functions/create>`_

Google Cloud Sql Operators
--------------------------

CloudSqlInstanceDatabaseCreateOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Creates a new database inside a Cloud SQL instance.

For parameter definition take a look at
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

CloudSqlInstanceDatabaseDeleteOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Deletes a database from a Cloud SQL instance.

For parameter definition take a look at
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

CloudSqlInstanceDatabasePatchOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Updates a resource containing information about a database inside a Cloud SQL instance
using patch semantics.
See: https://cloud.google.com/sql/docs/mysql/admin-api/how-tos/performance#patch

For parameter definition take a look at
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

CloudSqlInstanceDeleteOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Deletes a Cloud SQL instance in Google Cloud Platform.

For parameter definition take a look at
:class:`~airflow.contrib.operators.gcp_sql_operator.CloudSqlInstanceDeleteOperator`.

Arguments
"""""""""

Some arguments in the example DAG are taken from environment variables:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_sql.py
    :language: python
    :start-after: [START howto_operator_cloudsql_arguments]
    :end-before: [END howto_operator_cloudsql_arguments]

Using the operator
""""""""""""""""""

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_sql.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_cloudsql_delete]
    :end-before: [END howto_operator_cloudsql_delete]

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

.. _CloudSqlInstanceCreateOperator:

CloudSqlInstanceCreateOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Creates a new Cloud SQL instance in Google Cloud Platform.

For parameter definition take a look at
:class:`~airflow.contrib.operators.gcp_sql_operator.CloudSqlInstanceCreateOperator`.

If an instance with the same name exists, no action will be taken and the operator
will succeed.

Arguments
"""""""""

Some arguments in the example DAG are taken from environment variables:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_sql.py
    :language: python
    :start-after: [START howto_operator_cloudsql_arguments]
    :end-before: [END howto_operator_cloudsql_arguments]

Example body defining the instance:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_sql.py
    :language: python
    :start-after: [START howto_operator_cloudsql_create_body]
    :end-before: [END howto_operator_cloudsql_create_body]

Using the operator
""""""""""""""""""

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

See `Google Cloud SQL API documentation for insert <https://cloud.google
.com/sql/docs/mysql/admin-api/v1beta4/instances/insert>`_.


.. _CloudSqlInstancePatchOperator:

CloudSqlInstancePatchOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Updates settings of a Cloud SQL instance in Google Cloud Platform (partial update).

For parameter definition take a look at
:class:`~airflow.contrib.operators.gcp_sql_operator.CloudSqlInstancePatchOperator`.

This is a partial update, so only values for the settings specified in the body
will be set / updated. The rest of the existing instance's configuration will remain
unchanged.

Arguments
"""""""""

Some arguments in the example DAG are taken from environment variables:

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

See `Google Cloud SQL API documentation for patch <https://cloud.google
.com/sql/docs/mysql/admin-api/v1beta4/instances/patch>`_.
