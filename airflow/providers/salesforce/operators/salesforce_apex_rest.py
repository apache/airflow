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

from typing import TYPE_CHECKING

from airflow.models import BaseOperator
from airflow.providers.salesforce.hooks.salesforce import SalesforceHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class SalesforceApexRestOperator(BaseOperator):
    """
    Execute a APEX Rest API action

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SalesforceApexRestOperator`

    :param endpoint: The REST endpoint for the request.
    :param method: HTTP method for the request (default GET)
    :param payload: A dict of parameters to send in a POST / PUT request
    :param salesforce_conn_id: The :ref:`Salesforce Connection id <howto/connection:SalesforceHook>`.
    """

    def __init__(
        self,
        *,
        endpoint: str,
        method: str = "GET",
        payload: dict,
        salesforce_conn_id: str = "salesforce_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.endpoint = endpoint
        self.method = method
        self.payload = payload
        self.salesforce_conn_id = salesforce_conn_id

    def execute(self, context: Context) -> dict:
        """
        Makes an HTTP request to an APEX REST endpoint and pushes results to xcom.
        :param context: The task context during execution.
        :return: Apex response
        :rtype: dict
        """
        result: dict = {}
        sf_hook = SalesforceHook(salesforce_conn_id=self.salesforce_conn_id)
        conn = sf_hook.get_conn()
        execution_result = conn.apexecute(action=self.endpoint, method=self.method, data=self.payload)
        if self.do_xcom_push:
            result = execution_result

        return result
