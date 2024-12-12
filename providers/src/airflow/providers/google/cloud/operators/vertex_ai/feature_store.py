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
"""This module contains Google Vertex AI Feature Store operators."""

from __future__ import annotations

from typing import Any, Dict, Optional, Sequence, Union
from datetime import timedelta

from airflow.configuration import conf
from airflow.utils.context import Context

from airflow.providers.google.cloud.hooks.vertex_ai.feature_store import FeatureStoreHook
from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator

class SyncFeatureViewOperator(GoogleCloudBaseOperator):
    """Operator that syncs Vertex AI Feature Views"""

    template_fields: Sequence[str] = (
        'project_id',
        'location',
        'feature_online_store_id',
        'feature_view_id',
    )
    
    def __init__(
        self,
        *,
        project_id: str,
        location: str,
        feature_online_store_id: str,
        feature_view_id: str,
        gcp_conn_id: str = 'google_cloud_default',
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.location = location
        self.feature_online_store_id = feature_online_store_id
        self.feature_view_id = feature_view_id
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> Dict[str, Any]:
        """Executes the feature view sync operation"""

        self.hook = FeatureStoreHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

        response = self.hook.sync_feature_view(
            project_id=self.project_id,
            location=self.location,
            feature_online_store_id=self.feature_online_store_id,
            feature_view_id=self.feature_view_id,
        )
        
        return str(response.feature_view_sync)
    
class GetFeatureViewSyncOperator(GoogleCloudBaseOperator):
    """Operator that gets a Vertex AI Feature View Sync"""

    template_fields: Sequence[str] = (
        'location',
        'feature_view_sync_name',
    )
    
    def __init__(
        self,
        *,
        location: str,
        feature_view_sync_name: str,
        gcp_conn_id: str = 'google_cloud_default',
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.location = location
        self.feature_view_sync_name = feature_view_sync_name
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> Dict[str, Any]:
        """Executes the get feature view sync operation"""

        self.hook = FeatureStoreHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

        response = self.hook.get_feature_view_sync(
            location=self.location,
            feature_view_sync_name=self.feature_view_sync_name
        )
        
        report = {
            "name": self.feature_view_sync_name,
            "start_time": int(response.run_time.start_time.seconds)
        }

        if int(response.run_time.end_time.seconds) > 0:
            report['end_time'] = int(response.run_time.end_time.seconds)
            report['sync_summary'] = {
                "row_synced": int(response.sync_summary.row_synced),
                "total_slot": int(response.sync_summary.total_slot)
            }

        return report
