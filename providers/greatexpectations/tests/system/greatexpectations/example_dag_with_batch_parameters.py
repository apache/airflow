# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

import great_expectations.expectations as gxe

from airflow import DAG

try:  # airflow 3
    from airflow.sdk.bases.operator import chain
except ImportError:  # airflow 2
    from airflow.models.baseoperator import (  # type: ignore[import-not-found,no-redef]
        chain,
    )
from great_expectations import Checkpoint, ExpectationSuite, ValidationDefinition

from airflow.providers.greatexpectations.operators.validate_batch import GXValidateBatchOperator
from airflow.providers.greatexpectations.operators.validate_checkpoint import (
    GXValidateCheckpointOperator,
)

if TYPE_CHECKING:
    from great_expectations.core.batch_definition import BatchDefinition
    from great_expectations.data_context import AbstractDataContext

data_dir = Path(__file__).parent / "resources"
data_file = data_dir / "yellow_tripdata_sample_2019-01.csv"


# configuration functions
def configure_pandas_batch_definition(context: AbstractDataContext) -> BatchDefinition:
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


def configure_checkpoint(context: AbstractDataContext) -> Checkpoint:
    """This function takes a GX Context and returns a Checkpoint that
    can load our CSV files from the data directory, validate them
    against an ExpectationSuite, and run Actions."""
    # setup data source, asset, batch definition
    batch_definition = (
        context.data_sources.add_pandas_filesystem(name="Load Datasource", base_directory=data_dir)
        .add_csv_asset("Load Asset")
        .add_batch_definition_monthly(
            name="Load Batch Definition",
            regex=r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2}).csv",
        )
    )
    # setup expectation suite
    expectation_suite = context.suites.add(
        ExpectationSuite(
            name="Load ExpectationSuite",
            expectations=[
                gxe.ExpectTableRowCountToBeBetween(
                    min_value=9000,
                    max_value=11000,
                ),
                gxe.ExpectColumnValuesToNotBeNull(column="vendor_id"),
                gxe.ExpectColumnValuesToBeBetween(column="passenger_count", min_value=1, max_value=6),
            ],
        )
    )
    # setup validation definition
    validation_definition = context.validation_definitions.add(
        ValidationDefinition(
            name="Load Validation Definition",
            data=batch_definition,
            suite=expectation_suite,
        )
    )
    # setup checkpoint
    checkpoint = context.checkpoints.add(
        Checkpoint(
            name="Load Checkpoint",
            validation_definitions=[validation_definition],
            actions=[],
        )
    )
    return checkpoint


# define a consistent set of expectations we'll use throughout the pipeline
expectation_suite = ExpectationSuite(
    name="Taxi Data Expectations",
    expectations=[
        gxe.ExpectTableRowCountToBeBetween(
            min_value=9000,
            max_value=11000,
        ),
        gxe.ExpectColumnValuesToNotBeNull(column="vendor_id"),
        gxe.ExpectColumnValuesToBeBetween(column="passenger_count", min_value=1, max_value=6),
    ],
)


# Batch Parameters are available as DAG params, to be consumed directly by the
# operator through the context. Users can still provide batch_parameters on operator init
# (critical for validating data frames), but batch_parameters provided as DAG params should take precedence.
# To demo validation failure, use FAILURE_MONTH as a batch parameter instead of SUCCESS_MONTH
SUCCESS_MONTH = "01"
FAILURE_MONTH = "02"
_batch_parameters = {"year": "2019", "month": SUCCESS_MONTH}


ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "greatexpectations_example_dag_with_batch_parameters"

with DAG(
    dag_id=DAG_ID,
    schedule=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "great_expectations"],
    params={
        "gx_batch_parameters": _batch_parameters,
    },
) as dag:
    validate_extract = GXValidateBatchOperator(
        task_id="validate_extract",
        configure_batch_definition=configure_pandas_batch_definition,
        expect=expectation_suite,
    )

    validate_load = GXValidateCheckpointOperator(
        task_id="validate_load",
        configure_checkpoint=configure_checkpoint,
    )

    chain(
        validate_extract,
        validate_load,
    )


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
