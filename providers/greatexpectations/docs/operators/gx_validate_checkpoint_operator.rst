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



.. _howto/operator:GXValidateCheckpointOperator:

GXValidateCheckpointOperator
=============================

Use the :class:`~airflow.providers.greatexpectations.operators.validate_checkpoint.GXValidateCheckpointOperator`
to validate data using a GX Checkpoint. This operator supports all features of GX Core, including
`triggering actions <https://docs.greatexpectations.io/docs/core/trigger_actions_based_on_results/create_a_checkpoint_with_actions>`__
based on validation results.

Import the Operator
^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    from airflow.providers.greatexpectations.operators.validate_checkpoint import (
        GXValidateCheckpointOperator,
    )

Configure the Operator
^^^^^^^^^^^^^^^^^^^^^^

Instantiate the Operator with required and optional parameters:

.. code-block:: python

    from typing import TYPE_CHECKING

    if TYPE_CHECKING:
        from great_expectations import Checkpoint
        from great_expectations.data_context import AbstractDataContext


    def configure_checkpoint(context: AbstractDataContext) -> Checkpoint:
        """This function takes a GX Context and returns a Checkpoint that
        can load our CSV files from the data directory, validate them
        against an ExpectationSuite, and run Actions."""
        import great_expectations.expectations as gxe
        from great_expectations import Checkpoint, ExpectationSuite, ValidationDefinition

        # Setup data source, asset, batch definition
        batch_definition = (
            context.data_sources.add_pandas_filesystem(name="Load Datasource", base_directory=data_dir)
            .add_csv_asset("Load Asset")
            .add_batch_definition_monthly(
                name="Load Batch Definition",
                regex=r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2}).csv",
            )
        )

        # Setup expectation suite
        expectation_suite = context.suites.add(
            ExpectationSuite(
                name="Load ExpectationSuite",
                expectations=[
                    gxe.ExpectTableRowCountToBeBetween(
                        min_value=9000,
                        max_value=11000,
                    ),
                    gxe.ExpectColumnValuesToNotBeNull(column="vendor_id"),
                ],
            )
        )

        # Setup validation definition
        validation_definition = context.validation_definitions.add(
            ValidationDefinition(
                name="Load Validation Definition",
                data=batch_definition,
                suite=expectation_suite,
            )
        )

        # Setup checkpoint
        checkpoint = context.checkpoints.add(
            Checkpoint(
                name="Load Checkpoint",
                validation_definitions=[validation_definition],
                actions=[],
            )
        )
        return checkpoint


    my_checkpoint_operator = GXValidateCheckpointOperator(
        task_id="my_checkpoint_operator",
        configure_checkpoint=configure_checkpoint,
    )

Parameters
^^^^^^^^^^

* **task_id** (required): Alphanumeric name used in the Airflow UI and GX Cloud.
* **configure_checkpoint** (required): A callable that accepts an ``AbstractDataContext`` and returns a
  `Checkpoint <https://docs.greatexpectations.io/docs/core/trigger_actions_based_on_results/create_a_checkpoint_with_actions>`__,
  which orchestrates a ValidationDefinition, BatchDefinition, and ExpectationSuite. The Checkpoint can also
  specify a Result Format and trigger actions based on Validation Results.
* **batch_parameters** (optional): Dictionary that specifies a
  `time-based Batch of data <https://docs.greatexpectations.io/docs/core/define_expectations/retrieve_a_batch_of_test_data>`__
  to validate your Expectations against. Defaults to the first valid Batch found, which is the most recent Batch
  (with default sort ascending) or the oldest Batch if the Batch Definition has been configured to sort descending.
* **context_type** (optional): Accepts ``ephemeral``, ``cloud``, or ``file`` to set the Data Context used by the
  Operator. Defaults to ``ephemeral``, which does not persist results between runs. To save and view Validation
  Results in GX Cloud, use ``cloud`` and configure the GX Cloud connection (see below). To manage Validation
  Results yourself, use ``file`` and provide a ``configure_file_data_context`` function.
* **configure_file_data_context** (optional): A callable that returns a
  `FileDataContext <https://docs.greatexpectations.io/docs/core/set_up_a_gx_environment/create_a_data_context?context_type=file>`__.
  Applicable only when using a File Data Context (``context_type="file"``). By default, GX will write results
  in the configuration directory. If you are retrieving your FileDataContext from a remote location, you can
  yield the FileDataContext in the function and write the directory back to the remote after control is returned
  to the generator.
* **conn_id** (optional): The connection ID for GX Cloud. Only required when using ``context_type="cloud"``.

Example
^^^^^^^

For a complete end-to-end example, see:

.. exampleinclude:: /../../greatexpectations/tests/system/greatexpectations/example_great_expectations_dag.py
    :language: python
    :start-after: [START howto_operator_gx_validate_checkpoint]
    :end-before: [END howto_operator_gx_validate_checkpoint]
    :dedent: 4

GX Cloud Configuration
^^^^^^^^^^^^^^^^^^^^^^

If you use a Cloud Data Context (``context_type="cloud"``), you need to configure a GX Cloud connection.
See :ref:`howto/connection:gx_cloud` for details on how to configure the connection.

File Data Context Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you use a File Data Context (``context_type="file"``), pass the ``configure_file_data_context`` parameter.
This takes a callable that returns a
`FileDataContext <https://docs.greatexpectations.io/docs/core/set_up_a_gx_environment/create_a_data_context?context_type=file>`__.
By default, GX will write results in the configuration directory. If you are retrieving your FileDataContext from
a remote location, you can yield the FileDataContext in the ``configure_file_data_context`` function and write
the directory back to the remote after control is returned to the generator.


Validation Results
==================

The shape and content of the Validation Results depend on both the Operator type and the ``result_format`` parameter.

* ``GXValidateCheckpointOperator`` returns a
  `CheckpointResult <https://docs.greatexpectations.io/docs/reference/api/checkpoint/CheckpointResult_class>`__
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
    from airflow.providers.greatexpectations.operators.validate_checkpoint import GXValidateCheckpointOperator


    def handle_validation_failure(**context):
        """Retrieve validation results from a failed validation task."""
        # Get the validation result from XCom
        task_instance = context["task_instance"]
        validation_result = task_instance.xcom_pull(task_ids="validate_data", key="return_value")
        # Process the validation result
        print(f"Validation failed with result: {validation_result}")


    with DAG("example_validation_error_handling", ...) as dag:
        validate_task = GXValidateCheckpointOperator(
            task_id="validate_data",
            configure_checkpoint=configure_checkpoint,
        )

        handle_failure = PythonOperator(
            task_id="handle_failure",
            python_callable=handle_validation_failure,
            trigger_rule="all_failed",
        )

        validate_task >> handle_failure
