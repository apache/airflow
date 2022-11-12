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
"""This module contains a Google CloudRun operator."""
from __future__ import annotations

from typing import Sequence

from airflow.providers.google.cloud.hooks.cloud_run import CloudRunHook
from airflow.providers.http.operators.http import SimpleHttpOperator


class CloudRunOperator(SimpleHttpOperator):
    """
    Performs an authenticated call against CloudRun

    Under the hood, this operator use SimpleHttpOperator.
    Check the documentation of SimpleHttpOperator for extra options.

    This operator use the GCP connection to get a token and add it to http request Header.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudRunOperator`

    :param http_conn_id: The :ref:`CloudRun http connection<howto/connection:http>` to run
        the operator against
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    """

    template_fields: Sequence[str] = ("gcp_conn_id",)

    def __init__(
        self,
        *,
        gcp_conn_id: str = "google_cloud_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context):
        cloud_run = CloudRunHook(
            gcp_conn_id=self.gcp_conn_id,
            cloud_run_conn_id=self.http_conn_id,
        )

        authentication_header = cloud_run.get_conn()
        self.headers = {**authentication_header, **self.headers}
        return super().execute(context)
