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

In case you want to run deploy operator using a service account and get "forbidden 403"
errors, it means that your service account has not enough permissions set via IAM.

* First you need to Assign your Service Account "Cloud Functions Developer" role
* Make sure you grant the user the IAM Service Account User role on the Cloud Functions
Runtime service account. Typical way of doing it with gcloud is shown below - just
replace PROJECT_ID with ID of your project and SERVICE_ACCOUNT_EMAIL with the email id
of your service account.

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

The examples below use Airflow variables defined in order to show various variants and
combinations of default_args you can use. The variables are defined as follows:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_function_deploy_delete.py
    :language: python
    :start-after: [START howto_operator_gcf_deploy_variables]
    :end-before: [END howto_operator_gcf_deploy_variables]

With those variables one can define body of the request:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_function_deploy_delete.py
    :language: python
    :start-after: [START howto_operator_gcf_deploy_body]
    :end-before: [END howto_operator_gcf_deploy_body]

The default_args dictionary when you create DAG can be used to pass body and other
arguments:

.. literalinclude:: ../../airflow/contrib/example_dags/example_gcp_function_deploy_delete.py
    :language: python
    :start-after: [START howto_operator_gcf_deploy_args]
    :end-before: [END howto_operator_gcf_deploy_args]

Note that the neither the body nor default args are complete in the above examples.
Depending on the variables set there might be different variants on how to pass
source code related fields. Currently you can pass either
`sourceArchiveUrl`, `sourceRepository` or `sourceUploadUrl` as described in
`CloudFunction API specification <https://cloud.google.com/functions/docs/reference/rest/v1/projects.locations.functions#CloudFunction>`_.
Additionally default_args might contain `zip_path` parameter to run extra step
of uploading the source code before deploying it. In the last case you also need to
provide an empty `sourceUploadUrl` parameter in the body.

Example logic of setting the source code related fields based on variables defined above
is shown here:

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

In case you want to run deploy operator using a service account and get "forbidden 403"
errors, it means that your service account has not enough permissions set via IAM.

* First you need to Assign your Service Account "Cloud Functions Developer" role
* Make sure you grant the user the IAM Service Account User role on the Cloud Functions
Runtime service account. Typical way of doing it with gcloud is shown below - just
replace PROJECT_ID with ID of your project and SERVICE_ACCOUNT_EMAIL with the email id
of your service account.

.. code-block:: bash

  gcloud iam service-accounts add-iam-policy-binding \
    PROJECT_ID@appspot.gserviceaccount.com \
    --member="serviceAccount:[SERVICE_ACCOUNT_EMAIL]" \
    --role="roles/iam.serviceAccountUser"


See `Adding the IAM service agent user role to the runtime service <https://cloud.google.com/functions/docs/reference/iam/roles#adding_the_iam_service_agent_user_role_to_the_runtime_service_account>`_  for details

Also make sure that your service account has access to the source code of function
in case it should be downloaded. It might mean that you add Source Repository Viewer
role to the service account in case the source code is in Google Source Repository.
