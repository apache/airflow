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
"""Context actions for Great Expectations provider."""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Union

from airflow.providers.greatexpectations.common.constants import USER_AGENT_STR

if TYPE_CHECKING:
    from great_expectations import ExpectationSuite
    from great_expectations.core.batch_definition import BatchDefinition
    from great_expectations.core.expectation_validation_result import (
        ExpectationSuiteValidationResult,
    )
    from great_expectations.data_context import AbstractDataContext
    from great_expectations.expectations import Expectation

    from airflow.providers.greatexpectations.hooks.gx_cloud import GXCloudConfig


def run_validation_definition(
    task_id: str,
    expect: Expectation | ExpectationSuite,
    batch_definition: BatchDefinition,
    result_format: Literal["BOOLEAN_ONLY", "BASIC", "SUMMARY", "COMPLETE"] | None,
    batch_parameters: dict,
    gx_context: AbstractDataContext,
) -> ExpectationSuiteValidationResult:
    """Given a BatchDefinition and an Expectation or ExpectationSuite, ensure a
    ValidationDefinition and run it."""
    import great_expectations as gx

    if isinstance(expect, gx.expectations.Expectation):
        suite = gx.ExpectationSuite(name=task_id, expectations=[expect])
    else:
        suite = expect
    validation_definition = gx_context.validation_definitions.add_or_update(
        validation=gx.ValidationDefinition(
            name=task_id,
            suite=suite,
            data=batch_definition,
        ),
    )
    if result_format:
        result = validation_definition.run(
            batch_parameters=batch_parameters,
            result_format=result_format,
        )
    else:
        result = validation_definition.run(
            batch_parameters=batch_parameters,
        )
    return result


def load_data_context(
    context_type: Literal["ephemeral", "cloud"],
    gx_cloud_config: Union[GXCloudConfig, None],
) -> AbstractDataContext:
    import great_expectations as gx

    if context_type == "cloud" and gx_cloud_config:
        return gx.get_context(
            mode="cloud",
            cloud_access_token=gx_cloud_config.cloud_access_token,
            cloud_organization_id=gx_cloud_config.cloud_organization_id,
            cloud_workspace_id=gx_cloud_config.cloud_workspace_id,
            user_agent_str=USER_AGENT_STR,
        )
    else:
        # EphemeralDataContext or CloudDataContext with env vars
        return gx.get_context(
            mode=context_type,
            user_agent_str=USER_AGENT_STR,
        )
