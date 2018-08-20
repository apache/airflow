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
