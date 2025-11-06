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
"""Great Expectations checkpoint validation operator."""

from __future__ import annotations

import inspect
from collections.abc import Callable, Generator
from typing import TYPE_CHECKING, Literal, cast

from airflow.models import BaseOperator
from airflow.providers.greatexpectations.common.constants import USER_AGENT_STR
from airflow.providers.greatexpectations.common.errors import GXValidationFailed
from airflow.providers.greatexpectations.common.gx_context_actions import load_data_context
from airflow.providers.greatexpectations.hooks.gx_cloud import GXCloudHook

if TYPE_CHECKING:
    from great_expectations import Checkpoint
    from great_expectations.core.batch import BatchParameters
    from great_expectations.data_context import AbstractDataContext, FileDataContext

    from airflow.utils.context import Context


class GXValidateCheckpointOperator(BaseOperator):
    """
    An operator to use Great Expectations to run a Checkpoint in your Airflow DAG.

    Args:
        configure_checkpoint: A callable that returns a Checkpoint, which orchestrates a ValidationDefinition,
                BatchDefinition, and ExpectationSuite.
                The Checkpoint can also specify a Result Format and trigger actions based on Validation Results.
                For more information, see https://docs.greatexpectations.io/docs/core/trigger_actions_based_on_results/create_a_checkpoint_with_actions.
        batch_parameters: dictionary that specifies a time-based Batch of data  to validate your Expectations against.
            Defaults to the first valid Batch found, which is the most recent Batch (with default sort ascending)
            or the oldest Batch if the Batch Definition has been configured to sort descending.
            For more information see https://docs.greatexpectations.io/docs/core/define_expectations/retrieve_a_batch_of_test_data.
        context_type: accepts `ephemeral`, `cloud`, or `file` to set the DataContext used by the Operator.
            Defaults to `ephemeral`, which does not persist results between runs.
            To save and view Validation Results in GX Cloud, use `cloud` and include
            GX Cloud credentials in your environment. To manage Validation Results yourself, use `file` and provide the
            `configure_file_data_context` parameter.
        configure_file_data_context: callable that returns or yields a `FileDataContext`. Applicable only when using
            a FileDataContext. By default, GX will write results in the configuration directory. If you are retrieving
            your FileDataContext from a remote location, you can yield the FileDataContext in the
            `configure_file_data_context` function and write the directory back to the remote after control is returned
            to the generator.
    """

    def __init__(
        self,
        configure_checkpoint: Callable[[AbstractDataContext], Checkpoint],
        batch_parameters: BatchParameters | None = None,
        context_type: Literal["ephemeral", "cloud", "file"] = "ephemeral",
        configure_file_data_context: (
            Callable[[], FileDataContext] | Callable[[], Generator[FileDataContext, None, None]] | None
        ) = None,
        conn_id: str | None = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)

        if batch_parameters is None:
            self.batch_parameters = {}
        else:
            self.batch_parameters = batch_parameters
        if context_type == "file" and not configure_file_data_context:
            raise ValueError(
                "Parameter `configure_file_data_context` must be specified if `context_type` is `file`"
            )
        self.context_type = context_type
        self.configure_file_data_context = configure_file_data_context
        self.configure_checkpoint = configure_checkpoint
        self.conn_id = conn_id

    def execute(self, context: Context) -> None:
        gx_context: AbstractDataContext
        file_context_generator: Generator[FileDataContext, None, None] | None = None

        if self.context_type == "file":
            if not self.configure_file_data_context:
                raise ValueError(
                    "Parameter `configure_file_data_context` must be specified if `context_type` is `file`"
                )
            if inspect.isgeneratorfunction(self.configure_file_data_context):
                file_context_generator = self.configure_file_data_context()
                gx_context = self._get_value_from_generator(file_context_generator)
            else:
                file_context_fn = cast("Callable[[], FileDataContext]", self.configure_file_data_context)
                gx_context = file_context_fn()
            gx_context.set_user_agent_str(USER_AGENT_STR)
        else:
            if self.conn_id:
                gx_cloud_config = GXCloudHook(gx_cloud_conn_id=self.conn_id).get_conn()
            else:
                gx_cloud_config = None
            gx_context = load_data_context(gx_cloud_config=gx_cloud_config, context_type=self.context_type)
        checkpoint = self.configure_checkpoint(gx_context)

        runtime_batch_params = context.get("params", {}).get("gx_batch_parameters")  # type: ignore[call-overload]
        if runtime_batch_params:
            batch_parameters = runtime_batch_params
        else:
            batch_parameters = self.batch_parameters
        result = checkpoint.run(batch_parameters=batch_parameters)

        if file_context_generator:
            self._allow_generator_teardown(file_context_generator)

        result_dict = result.describe_dict()
        context["ti"].xcom_push(key="return_value", value=result_dict)
        if not result.success:
            raise GXValidationFailed(result_dict, self.task_id)

    def _get_value_from_generator(self, generator: Generator[FileDataContext, None, None]) -> FileDataContext:
        try:
            return next(generator)
        except StopIteration:
            raise RuntimeError("Generator must yield exactly once; did not yield")

    def _allow_generator_teardown(self, generator: Generator) -> None:
        """
        Run the generator to completion to allow for any cleanup/teardown.

        Also does some error handling to ensure the generator doesn't yield more than once.
        """
        try:
            # Check if we have another yield (this is an error case)
            next(generator)
        except StopIteration:
            pass
        else:
            raise RuntimeError("Generator must yield exactly once; yielded more than once")
