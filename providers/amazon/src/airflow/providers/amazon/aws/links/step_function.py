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

from urllib.parse import quote_plus

from airflow.providers.amazon.aws.links.base_aws import BASE_AWS_CONSOLE_LINK, BaseAwsLink


class StateMachineDetailsLink(BaseAwsLink):
    """Helper class for constructing link to State Machine details page."""

    name = "State Machine Details"
    key = "_state_machine_details"
    format_str = (
        BASE_AWS_CONSOLE_LINK + "/states/home?region={region_name}#/statemachines/view/{state_machine_arn}"
    )

    def format_link(self, *, state_machine_arn: str | None = None, **kwargs) -> str:
        if not state_machine_arn:
            return ""
        return super().format_link(state_machine_arn=quote_plus(state_machine_arn), **kwargs)


class StateMachineExecutionsDetailsLink(BaseAwsLink):
    """Helper class for constructing link to State Machine Execution details page."""

    name = "State Machine Executions Details"
    key = "_state_machine_executions_details"
    format_str = (
        BASE_AWS_CONSOLE_LINK + "/states/home?region={region_name}#/v2/executions/details/{execution_arn}"
    )

    def format_link(self, *, execution_arn: str | None = None, **kwargs) -> str:
        if not execution_arn:
            return ""
        return super().format_link(execution_arn=quote_plus(execution_arn), **kwargs)
