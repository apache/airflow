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

from collections.abc import Callable
from typing import TYPE_CHECKING, Literal

from airflow.models import BaseOperator
from airflow.providers.greatexpectations.common.errors import GXValidationFailed
from airflow.providers.greatexpectations.common.gx_context_actions import (
    load_data_context,
    run_validation_definition,
)
from airflow.providers.greatexpectations.hooks.gx_cloud import GXCloudHook

if TYPE_CHECKING:
    from great_expectations import ExpectationSuite
    from great_expectations.core.batch import BatchParameters
    from great_expectations.core.batch_definition import BatchDefinition
    from great_expectations.data_context import AbstractDataContext
    from great_expectations.expectations import Expectation

    from airflow.utils.context import Context


class GXValidateBatchOperator(BaseOperator):
    """
    An operator to use Great Expectations to validate Expectations against a Batch of data in your Airflow DAG.

    Args:
        task_id: Airflow task ID. Alphanumeric name used in the Airflow UI and to name components in GX Cloud.
        configure_batch_definition: A callable that returns a BatchDefinition to configure GX to read your data. For more information, see https://docs.greatexpectations.io/docs/core/connect_to_data/filesystem_data/#create-a-batch-definition.
        configure_expectations: A callable that accepts an AbstractDataContext and returns an Expectation or ExpectationSuite to validate against the Batch. Available Expectations can be found at https://greatexpectations.io/expectations.
        batch_parameters: dictionary that specifies a time-based Batch of data to validate your Expectations against. Defaults to the first valid Batch found, which is the most recent Batch (with default sort ascending) or the oldest Batch if the Batch Definition has been configured to sort descending. For more information see https://docs.greatexpectations.io/docs/core/define_expectations/retrieve_a_batch_of_test_data.
        result_format: control the verbosity of returned Validation Results. Possible values are "BOOLEAN_ONLY", "BASIC", "SUMMARY", "COMPLETE". Defaults to "SUMMARY". See https://docs.greatexpectations.io/docs/core/trigger_actions_based_on_results/choose_a_result_format for more information.
        context_type: accepts `ephemeral` or `cloud` to set the DataContext used by the Operator. Defaults to `ephemeral`, which does not persist results between runs. To save and view Validation Results in GX Cloud, use `cloud` and include GX Cloud credentials in your environment.
    """

    def __init__(
        self,
        configure_batch_definition: Callable[[AbstractDataContext], BatchDefinition],
        configure_expectations: Callable[[AbstractDataContext], Expectation | ExpectationSuite],
        batch_parameters: BatchParameters | None = None,
        context_type: Literal["ephemeral", "cloud"] = "ephemeral",
        result_format: (Literal["BOOLEAN_ONLY", "BASIC", "SUMMARY", "COMPLETE"] | None) = None,
        conn_id: str | None = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)

        if batch_parameters is None:
            self.batch_parameters = {}
        else:
            self.batch_parameters = batch_parameters
        self.context_type = context_type
        self.configure_batch_definition = configure_batch_definition
        self.configure_expectations = configure_expectations
        self.result_format = result_format
        self.conn_id = conn_id

    def execute(self, context: Context) -> None:
        if self.conn_id:
            gx_cloud_config = GXCloudHook(gx_cloud_conn_id=self.conn_id).get_conn()
        else:
            gx_cloud_config = None
        gx_context = load_data_context(gx_cloud_config=gx_cloud_config, context_type=self.context_type)
        batch_definition = self.configure_batch_definition(gx_context)
        expect = self.configure_expectations(gx_context)

        runtime_batch_params = context.get("params", {}).get("gx_batch_parameters")  # type: ignore[call-overload]
        if runtime_batch_params:
            batch_parameters = runtime_batch_params
        else:
            batch_parameters = self.batch_parameters
        result = run_validation_definition(
            task_id=self.task_id,
            expect=expect,
            batch_definition=batch_definition,
            result_format=self.result_format,
            batch_parameters=batch_parameters,
            gx_context=gx_context,
        )
        result_dict = result.describe_dict()
        context["ti"].xcom_push(key="return_value", value=result_dict)
        if not result.success:
            raise GXValidationFailed(result_dict, self.task_id)
