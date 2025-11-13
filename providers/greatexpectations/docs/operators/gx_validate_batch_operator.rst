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

.. _howto/operator:GXValidateBatchOperator:


GXValidateBatchOperator
========================

Use the :class:`~airflow.providers.greatexpectations.operators.validate_batch.GXValidateBatchOperator`
to validate Expectations against data that is not in memory. This operator configures GX to connect to
your data source by defining a BatchDefinition.

Import the Operator
^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    from airflow.providers.greatexpectations.operators.validate_batch import (
        GXValidateBatchOperator,
    )

Configure the Operator
^^^^^^^^^^^^^^^^^^^^^^

Instantiate the Operator with required and optional parameters:

.. code-block:: python

    from typing import TYPE_CHECKING

    if TYPE_CHECKING:
        from great_expectations.core.batch_definition import BatchDefinition
        from great_expectations.data_context import AbstractDataContext


    def configure_batch_definition(context: AbstractDataContext) -> BatchDefinition:
        """This function takes a GX Context and returns a BatchDefinition that
        can load our CSV files from the data directory."""
        data_source = context.data_sources.add_pandas_filesystem(
            name="Extract Data Source",
            base_directory=data_dir,
        )
        asset = data_source.add_csv_asset(name="Extract CSV Asset")
        batch_definition = asset.add_batch_definition_monthly(
            name="Extract Batch Definition",
            regex=r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2}).csv",
        )
        return batch_definition


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


    my_batch_operator = GXValidateBatchOperator(
        task_id="my_batch_operator",
        configure_batch_definition=configure_batch_definition,
        configure_expectations=configure_expectations,
    )

Parameters
^^^^^^^^^^

* **task_id** (required): Alphanumeric name used in the Airflow UI and GX Cloud.
* **configure_batch_definition** (required): A callable that accepts an ``AbstractDataContext`` and returns a
  `BatchDefinition <https://docs.greatexpectations.io/docs/core/connect_to_data/filesystem_data/#create-a-batch-definition>`__
  to configure GX to read your data.
* **configure_expectations** (required): A callable that accepts an ``AbstractDataContext`` and returns an
  ``Expectation`` or ``ExpectationSuite`` to validate against your data. Available Expectations can be found at
  https://greatexpectations.io/expectations.
* **batch_parameters** (optional): Dictionary that specifies a
  `time-based Batch of data <https://docs.greatexpectations.io/docs/core/define_expectations/retrieve_a_batch_of_test_data>`__
  to validate your Expectations against. Defaults to the first valid Batch found, which is the most recent Batch
  (with default sort ascending) or the oldest Batch if the Batch Definition has been configured to sort descending.
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

.. exampleinclude:: /../../greatexpectations/tests/system/greatexpectations/example_dag_with_batch_parameters.py
    :language: python
    :start-after: [START howto_operator_gx_validate_batch]
    :end-before: [END howto_operator_gx_validate_batch]
    :dedent: 4

GX Cloud Configuration
^^^^^^^^^^^^^^^^^^^^^^

If you use a Cloud Data Context (``context_type="cloud"``), you need to configure a GX Cloud connection.
See :ref:`howto/connection:gx_cloud` for details on how to configure the connection.


Validation Results
==================

The shape and content of the Validation Results depend on both the Operator type and the ``result_format`` parameter.

* The ``GXValidateBatchOperator`` returns a serialized
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
    from airflow.providers.greatexpectations.operators.validate_batch import GXValidateBatchOperator


    def handle_validation_failure(**context):
        """Retrieve validation results from a failed validation task."""
        # Get the validation result from XCom
        task_instance = context["task_instance"]
        validation_result = task_instance.xcom_pull(task_ids="validate_data", key="return_value")
        # Process the validation result
        print(f"Validation failed with result: {validation_result}")


    with DAG("example_validation_error_handling", ...) as dag:
        validate_task = GXValidateBatchOperator(
            task_id="validate_data",
            configure_batch_definition=configure_batch_definition,
            configure_expectations=configure_expectations,
        )

        handle_failure = PythonOperator(
            task_id="handle_failure",
            python_callable=handle_validation_failure,
            trigger_rule="all_failed",
        )

        validate_task >> handle_failure
