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


.. _howto/operator:GXValidateDataFrameOperator:

GXValidateDataFrameOperator
============================

Use the :class:`~airflow.providers.greatexpectations.operators.validate_dataframe.GXValidateDataFrameOperator`
to validate Expectations against a DataFrame in your Airflow DAG. This operator is ideal when your data
is already loaded in memory as a Spark or Pandas DataFrame.

Import the Operator
^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    from airflow.providers.greatexpectations.operators.validate_dataframe import (
        GXValidateDataFrameOperator,
    )

Configure the Operator
^^^^^^^^^^^^^^^^^^^^^^

Instantiate the Operator with required and optional parameters:

.. code-block:: python

    from typing import TYPE_CHECKING

    if TYPE_CHECKING:
        from pandas import DataFrame
        from great_expectations.data_context import AbstractDataContext


    def configure_data_frame() -> DataFrame:
        import pandas as pd

        # Airflow best practice is to not import heavy dependencies at the top level
        return pd.read_csv(my_data_file)


    def configure_expectations(context: AbstractDataContext):
        """Return the expectation suite for validation."""
        import great_expectations.expectations as gxe
        from great_expectations import ExpectationSuite

        return ExpectationSuite(
            name="My Expectation Suite",
            expectations=[
                gxe.ExpectTableRowCountToBeBetween(
                    min_value=9000,
                    max_value=11000,
                ),
                gxe.ExpectColumnValuesToNotBeNull(column="vendor_id"),
            ],
        )


    my_data_frame_operator = GXValidateDataFrameOperator(
        task_id="my_data_frame_operator",
        configure_dataframe=configure_data_frame,
        configure_expectations=configure_expectations,
    )

Parameters
^^^^^^^^^^

* **task_id** (required): Alphanumeric name used in the Airflow UI and GX Cloud.
* **configure_dataframe** (required): A callable which returns the DataFrame to be validated.
* **configure_expectations** (required): A callable that accepts an ``AbstractDataContext`` and returns an
  ``Expectation`` or ``ExpectationSuite`` to validate against the DataFrame. Available Expectations can be
  found at https://greatexpectations.io/expectations.
* **result_format** (optional): Control the verbosity of returned Validation Results. Accepts ``BOOLEAN_ONLY``,
  ``BASIC``, ``SUMMARY``, or ``COMPLETE``. Defaults to ``SUMMARY``. See the
  `Result Format documentation <https://docs.greatexpectations.io/docs/core/trigger_actions_based_on_results/choose_a_result_format>`__
  for more information.
* **context_type** (optional): Accepts ``ephemeral`` or ``cloud`` to set the Data Context used by the Operator.
  Defaults to ``ephemeral``, which does not persist results between runs. To save and view Validation Results
  in GX Cloud, use ``cloud`` and configure the GX Cloud connection (see below).
* **conn_id** (optional): The connection ID for GX Cloud. Only required when using ``context_type="cloud"``.

Example
^^^^^^^

For a complete end-to-end example, see:

.. exampleinclude:: /../../greatexpectations/tests/system/greatexpectations/example_great_expectations_dag.py
    :language: python
    :start-after: [START howto_operator_gx_validate_dataframe]
    :end-before: [END howto_operator_gx_validate_dataframe]
    :dedent: 4

GX Cloud Configuration
^^^^^^^^^^^^^^^^^^^^^^

If you use a Cloud Data Context (``context_type="cloud"``), you need to configure a GX Cloud connection.
See :ref:`howto/connection:gx_cloud` for details on how to configure the connection.

You can either:

1. Create a connection with ID ``gx_cloud_default`` (the default), or
2. Specify a custom connection ID using the ``conn_id`` parameter.


Validation Results
==================

The shape and content of the Validation Results depend on both the Operator type and the ``result_format`` parameter.

* The ``GXValidateDataFrameOperator`` returns a serialized
  `ExpectationSuiteValidationResult <https://docs.greatexpectations.io/docs/reference/api/core/expectationsuitevalidationresult_class/>`__
* The included fields depend on the
  `Result Format verbosity <https://docs.greatexpectations.io/docs/core/trigger_actions_based_on_results/choose_a_result_format/?results=verbosity#validation-results-reference-tables>`__

For detailed information about validation results, see the
`Great Expectations documentation on result formats <https://docs.greatexpectations.io/docs/core/trigger_actions_based_on_results/choose_a_result_format>`__.


Error Handling
==============

When one of the GX operators fails validation, it raises a ``GXValidationFailed`` exception. This exception provides:

* **Error message**: Contains a summary of the failed expectations
* **XCom key**: The error message includes the key to the task XCom, which holds the complete failing validation result

You can access the full validation result from downstream tasks or for debugging purposes by retrieving it from XCom
using the key provided in the error message.

Example of handling validation failures:

.. code-block:: python

    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from airflow.providers.greatexpectations.operators.validate_dataframe import GXValidateDataFrameOperator


    def handle_validation_failure(**context):
        """Retrieve validation results from a failed validation task."""
        # Get the validation result from XCom
        task_instance = context["task_instance"]
        validation_result = task_instance.xcom_pull(task_ids="validate_data", key="return_value")
        # Process the validation result
        print(f"Validation failed with result: {validation_result}")


    with DAG("example_validation_error_handling", ...) as dag:
        validate_task = GXValidateDataFrameOperator(
            task_id="validate_data",
            configure_data_frame=configure_data_frame,
            configure_expectations=configure_expectations,
        )

        handle_failure = PythonOperator(
            task_id="handle_failure",
            python_callable=handle_validation_failure,
            trigger_rule="all_failed",
        )

        validate_task >> handle_failure
