#
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

from typing import TYPE_CHECKING, Any, Callable, Sequence

from airflow.providers.atlassian.jira.hooks.jira import JiraHook
from airflow.sensors.base import BaseSensorOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class JiraSensor(BaseSensorOperator):
    """
    Monitors a jira ticket for any change.

    :param jira_conn_id: reference to a pre-defined Jira Connection
    :param method_name: method name from atlassian-python-api JIRA sdk to execute
    :param method_params: parameters for the method method_name
    :param result_processor: function that return boolean and act as a sensor response
    """

    def __init__(
        self,
        *,
        method_name: str,
        jira_conn_id: str = "jira_default",
        method_params: dict | None = None,
        result_processor: Callable | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.jira_conn_id = jira_conn_id
        self.result_processor = None
        if result_processor is not None:
            self.result_processor = result_processor
        self.method_name = method_name
        self.method_params = method_params

    def poke(self, context: Context) -> Any:
        hook = JiraHook(jira_conn_id=self.jira_conn_id)
        resource = hook.get_conn()
        jira_result = getattr(resource, self.method_name)(**self.method_params)
        if self.result_processor is None:
            return jira_result
        return self.result_processor(jira_result)


class JiraTicketSensor(JiraSensor):
    """
    Monitors a jira ticket for given change in terms of function.

    :param jira_conn_id: reference to a pre-defined Jira Connection
    :param ticket_id: id of the ticket to be monitored
    :param field: field of the ticket to be monitored
    :param expected_value: expected value of the field
    :param result_processor: function that return boolean and act as a sensor response
    """

    template_fields: Sequence[str] = ("ticket_id",)

    def __init__(
        self,
        *,
        jira_conn_id: str = "jira_default",
        ticket_id: str | None = None,
        field: str | None = None,
        expected_value: str | None = None,
        field_checker_func: Callable | None = None,
        **kwargs,
    ) -> None:
        self.jira_conn_id = jira_conn_id
        self.ticket_id = ticket_id
        self.field = field
        self.expected_value = expected_value
        if field_checker_func is None:
            field_checker_func = self.issue_field_checker

        super().__init__(
            jira_conn_id=jira_conn_id,
            method_name="issue",
            result_processor=field_checker_func,
            **kwargs,
        )

    def poke(self, context: Context) -> Any:
        self.log.info("Jira Sensor checking for change in ticket: %s", self.ticket_id)

        self.method_name = "issue"
        self.method_params = {"key": self.ticket_id, "fields": self.field}
        return JiraSensor.poke(self, context=context)

    def issue_field_checker(self, jira_result: dict) -> bool | None:
        """Check issue using different conditions to prepare to evaluate sensor."""
        result = None
        if (
            jira_result is not None
            and self.field is not None
            and self.expected_value is not None
        ):
            field_val = jira_result.get("fields", {}).get(self.field, None)
            if field_val is not None:
                if isinstance(field_val, list):
                    result = self.expected_value in field_val
                elif isinstance(field_val, str):
                    result = self.expected_value.lower() == field_val.lower()
                elif isinstance(field_val, dict) and field_val.get("name", None):
                    result = (
                        self.expected_value.lower() == field_val.get("name", "").lower()
                    )
                else:
                    self.log.warning(
                        "Not implemented checker for issue field %s which "
                        "is neither string nor list nor Jira Resource",
                        self.field,
                    )

        if result is True:
            self.log.info(
                "Issue field %s has expected value %s, returning success",
                self.field,
                self.expected_value,
            )
        else:
            self.log.info(
                "Issue field %s don't have expected value %s yet.",
                self.field,
                self.expected_value,
            )
        return result
