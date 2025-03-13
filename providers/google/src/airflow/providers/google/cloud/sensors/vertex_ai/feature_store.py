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

"""This module contains a Vertex AI Feature Store sensor."""

from __future__ import annotations

import time
from collections.abc import Sequence
from typing import TYPE_CHECKING

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.vertex_ai.feature_store import FeatureStoreHook
from airflow.sensors.base import BaseSensorOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class FeatureViewSyncSensor(BaseSensorOperator):
    """
    Sensor to monitor the state of a Vertex AI Feature View sync operation.

    :param feature_view_sync_name: The name of the feature view sync operation to monitor. (templated)
    :param location: Required. The Cloud region in which to handle the request. (templated)
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud Platform.
    :param wait_timeout: How many seconds to wait for sync to complete.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials.
    """

    template_fields: Sequence[str] = ("location", "feature_view_sync_name")
    ui_color = "#f0eee4"

    def __init__(
        self,
        *,
        feature_view_sync_name: str,
        location: str,
        gcp_conn_id: str = "google_cloud_default",
        wait_timeout: int | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.feature_view_sync_name = feature_view_sync_name
        self.location = location
        self.gcp_conn_id = gcp_conn_id
        self.wait_timeout = wait_timeout
        self.impersonation_chain = impersonation_chain
        self.start_sensor_time: float | None = None

    def execute(self, context: Context) -> None:
        self.start_sensor_time = time.monotonic()
        super().execute(context)

    def _duration(self):
        return time.monotonic() - self.start_sensor_time

    def poke(self, context: Context) -> bool:
        hook = FeatureStoreHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

        try:
            response = hook.get_feature_view_sync(
                location=self.location,
                feature_view_sync_name=self.feature_view_sync_name,
            )

            # Check if the sync has completed by verifying end_time exists
            if response.get("end_time", 0) > 0:
                self.log.info(
                    "Feature View sync %s completed. Rows synced: %d, Total slots: %d",
                    self.feature_view_sync_name,
                    int(response.get("sync_summary", "").get("row_synced", "")),
                    int(response.get("sync_summary", "").get("total_slot", "")),
                )
                return True

            if self.wait_timeout and self._duration() > self.wait_timeout:
                raise AirflowException(
                    f"Timeout: Feature View sync {self.feature_view_sync_name} "
                    f"not completed after {self.wait_timeout}s"
                )

            self.log.info("Waiting for Feature View sync %s to complete.", self.feature_view_sync_name)
            return False

        except Exception as e:
            if self.wait_timeout and self._duration() > self.wait_timeout:
                raise AirflowException(
                    f"Timeout: Feature View sync {self.feature_view_sync_name} "
                    f"not completed after {self.wait_timeout}s"
                )
            self.log.info("Error checking sync status, will retry: %s", str(e))
            return False
