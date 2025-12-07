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

from typing import Any

import requests

from airflow.exceptions import AirflowException
from airflow.providers.common.compat.sdk import BaseHook

OK_STATUS_CODE = 202


class OpenFaasHook(BaseHook):
    """
    Interact with OpenFaaS to query, deploy, invoke and update function.

    :param function_name: Name of the function, Defaults to None
    :param conn_id: OpenFaaS connection to use, defaults to ``open_faas_default``
        for example host : http://openfaas.faas.com
    """

    conn_name_attr = "conn_id"
    default_conn_name = "open_faas_default"
    conn_type = "openfaas"
    hook_name = "OpenFaaS"

    GET_FUNCTION = "/system/function/"
    INVOKE_ASYNC_FUNCTION = "/async-function/"
    INVOKE_FUNCTION = "/function/"
    DEPLOY_FUNCTION = "/system/functions"
    UPDATE_FUNCTION = "/system/functions"

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom field behaviour."""
        return {
            "hidden_fields": ["schema", "port", "login", "password", "extra"],
            "relabeling": {},
        }

    def __init__(self, function_name=None, conn_id: str = "open_faas_default", *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.function_name = function_name
        self.conn_id = conn_id

    def get_conn(self):
        conn = self.get_connection(self.conn_id)
        return conn

    def deploy_function(self, overwrite_function_if_exist: bool, body: dict[str, Any]) -> None:
        """Deploy OpenFaaS function."""
        if overwrite_function_if_exist:
            self.log.info("Function already exist %s going to update", self.function_name)
            self.update_function(body)
        else:
            url = self.get_conn().host + self.DEPLOY_FUNCTION
            self.log.info("Deploying function %s", url)
            response = requests.post(url, body)
            if response.status_code != OK_STATUS_CODE:
                self.log.error("Response status %d", response.status_code)
                self.log.error("Failed to deploy")
                raise AirflowException("failed to deploy")
            self.log.info("Function deployed %s", self.function_name)

    def invoke_async_function(self, body: dict[str, Any]) -> None:
        """Invoke function asynchronously."""
        url = self.get_conn().host + self.INVOKE_ASYNC_FUNCTION + self.function_name
        self.log.info("Invoking function asynchronously %s", url)
        response = requests.post(url, body)
        if response.ok:
            self.log.info("Invoked %s", self.function_name)
        else:
            self.log.error("Response status %d", response.status_code)
            raise AirflowException("failed to invoke function")

    def invoke_function(self, body: dict[str, Any]) -> None:
        """Invoke function synchronously. This will block until function completes and returns."""
        url = self.get_conn().host + self.INVOKE_FUNCTION + self.function_name
        self.log.info("Invoking function synchronously %s", url)
        response = requests.post(url, body)
        if response.ok:
            self.log.info("Invoked %s", self.function_name)
            self.log.info("Response code %s", response.status_code)
            self.log.info("Response %s", response.text)
        else:
            self.log.error("Response status %d", response.status_code)
            raise AirflowException("failed to invoke function")

    def update_function(self, body: dict[str, Any]) -> None:
        """Update OpenFaaS function."""
        url = self.get_conn().host + self.UPDATE_FUNCTION
        self.log.info("Updating function %s", url)
        response = requests.put(url, body)
        if response.status_code != OK_STATUS_CODE:
            self.log.error("Response status %d", response.status_code)
            self.log.error("Failed to update response %s", response.content.decode("utf-8"))
            raise AirflowException("failed to update " + self.function_name)
        self.log.info("Function was updated")

    def does_function_exist(self) -> bool:
        """Whether OpenFaaS function exists or not."""
        url = self.get_conn().host + self.GET_FUNCTION + self.function_name

        response = requests.get(url)
        if response.ok:
            return True
        self.log.error("Failed to find function %s", self.function_name)
        return False
