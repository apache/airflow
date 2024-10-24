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

from typing import TYPE_CHECKING, Sequence

from airflow.exceptions import AirflowFailException
from airflow.providers.google.cloud.hooks.financial_services import FinancialServicesHook
from airflow.sensors.base import BaseSensorOperator, PokeReturnValue

if TYPE_CHECKING:
    from airflow.utils.context import Context


class FinancialServicesOperationSensor(BaseSensorOperator):
    """
    Check status of AML AI operation.

    :param operation_resource_uri: URI of the operation (format:
        'projects/<Project ID>/locations/<Location>/operations/<Operation ID>)
    :param gcp_conn_id: Identifier of connection to Google Cloud Platform.
        Defaults to "google_cloud_default".
    """

    # [START howto_sensor_financial_services_operation_template_fields]
    template_fields: Sequence[str] = ("operation_resource_uri",)
    # [END howto_sensor_financial_services_operation_template_fields]

    def __init__(
        self,
        operation_resource_uri: str,
        gcp_conn_id: str = "google_cloud_default",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.operation_resource_uri = operation_resource_uri
        self.gcp_conn_id = gcp_conn_id

    def poke(self, context: Context) -> PokeReturnValue:
        hook = FinancialServicesHook(
            gcp_conn_id=self.gcp_conn_id,
        )
        operation = hook.get_operation(operation_resource_uri=self.operation_resource_uri)
        if "error" in operation.keys():
            raise AirflowFailException(operation["error"])
        return PokeReturnValue(is_done=operation["done"])
