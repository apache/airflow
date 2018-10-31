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

Google Cloud Platform Operators
-------------------------------

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

GceInstanceStartOperator
^^^^^^^^^^^^^^^^^^^^^^^^

Allows to start an existing Google Compute Engine instance.

In this example parameter values are extracted from Airflow variables.
Moreover, the ``default_args`` dict is used to pass common arguments to all operators in a single DAG.

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_compute.py
    :language: python
    :start-after: [START howto_operator_gce_args]
    :end-before: [END howto_operator_gce_args]


Define the :class:`~airflow.contrib.operators.gcp_compute_operator
.GceInstanceStartOperator` by passing the required arguments to the constructor.

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_compute.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gce_start]
    :end-before: [END howto_operator_gce_start]

GceInstanceStopOperator
^^^^^^^^^^^^^^^^^^^^^^^

Allows to stop an existing Google Compute Engine instance.

For parameter definition take a look at :class:`~airflow.contrib.operators.gcp_compute_operator.GceInstanceStartOperator` above.

Define the :class:`~airflow.contrib.operators.gcp_compute_operator
.GceInstanceStopOperator` by passing the required arguments to the constructor.

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_compute.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gce_stop]
    :end-before: [END howto_operator_gce_stop]

GceSetMachineTypeOperator
^^^^^^^^^^^^^^^^^^^^^^^^^

Allows to change the machine type for a stopped instance to the specified machine type.

For parameter definition take a look at :class:`~airflow.contrib.operators.gcp_compute_operator.GceInstanceStartOperator` above.

Define the :class:`~airflow.contrib.operators.gcp_compute_operator
.GceSetMachineTypeOperator` by passing the required arguments to the constructor.

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_compute.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gce_set_machine_type]
    :end-before: [END howto_operator_gce_set_machine_type]


GcfFunctionDeleteOperator
^^^^^^^^^^^^^^^^^^^^^^^^^

Use the ``default_args`` dict to pass arguments to the operator.

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_function_delete.py
    :language: python
    :start-after: [START howto_operator_gcf_delete_args]
    :end-before: [END howto_operator_gcf_delete_args]


Use the :class:`~airflow.contrib.operators.gcp_function_operator.GcfFunctionDeleteOperator`
to delete a function from Google Cloud Functions.

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_function_delete.py
    :language: python
    :start-after: [START howto_operator_gcf_delete]
    :end-before: [END howto_operator_gcf_delete]

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

GcfFunctionDeployOperator
^^^^^^^^^^^^^^^^^^^^^^^^^

Use the :class:`~airflow.contrib.operators.gcp_function_operator.GcfFunctionDeployOperator`
to deploy a function from Google Cloud Functions.

The following examples of Airflow variables show various variants and combinations
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

When you create a DAG, the default_args dictionary can be used to pass the body and
other arguments:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_function_deploy_delete.py
    :language: python
    :start-after: [START howto_operator_gcf_deploy_args]
    :end-before: [END howto_operator_gcf_deploy_args]

Note that the neither the body nor the default args are complete in the above examples.
Depending on the set variables, there might be different variants on how to pass source
code related fields. Currently, you can pass either sourceArchiveUrl, sourceRepository
or sourceUploadUrl as described in the
`CloudFunction API specification <https://cloud.google.com/functions/docs/reference/rest/v1/projects.locations.functions#CloudFunction>`_.
Additionally, default_args might contain zip_path parameter to run the extra step of
uploading the source code before deploying it. In the last case, you also need to
provide an empty `sourceUploadUrl` parameter in the body.

Based on the variables defined above, example logic of setting the source code
related fields is shown here:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_function_deploy_delete.py
    :language: python
    :start-after: [START howto_operator_gcf_deploy_variants]
    :end-before: [END howto_operator_gcf_deploy_variants]

The code to create the operator:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_function_deploy_delete.py
    :language: python
    :start-after: [START howto_operator_gcf_deploy]
    :end-before: [END howto_operator_gcf_deploy]

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

CloudSqlInstanceDeleteOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Deletes a Cloud SQL instance in Google Cloud Platform.

For parameter definition take a look at
:class:`~airflow.contrib.operators.gcp_sql_operator.CloudSqlInstanceDeleteOperator`.

Arguments
"""""""""

Some arguments in the example DAG are taken from Airflow variables:

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

Some arguments in the example DAG are taken from Airflow variables:

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

Some arguments in the example DAG are taken from Airflow variables:

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
