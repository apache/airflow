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
"""This module contains Google Dataflow links."""
from __future__ import annotations

from typing import TYPE_CHECKING

from airflow.models import BaseOperator
from airflow.providers.google.cloud.links.base import BaseGoogleLink

if TYPE_CHECKING:
    from airflow.utils.context import Context
DATAFORM_BASE_LINK = "https://pantheon.corp.google.com/bigquery/dataform"
DATAFORM_WORKFLOW_INVOCATION_LINK = (
    DATAFORM_BASE_LINK
    + "/locations/{region}/repositories/{repository_id}/workflows/"
    + "{workflow_invocation_id}?project={project_id}"
)


class DataformWorkflowInvocationLink(BaseGoogleLink):
    """Helper class for constructing Dataflow Job Link"""

    name = "Dataform Workflow Invocation"
    key = "dataform_workflow_invocation_config"
    format_str = DATAFORM_WORKFLOW_INVOCATION_LINK

    @staticmethod
    def persist(
        operator_instance: BaseOperator,
        context: Context,
        project_id: str,
        region: str,
        repository_id: str,
        workflow_invocation_id: str,
    ):
        operator_instance.xcom_push(
            context,
            key=DataformWorkflowInvocationLink.key,
            value={
                "project_id": project_id,
                "region": region,
                "repository_id": repository_id,
                "workflow_invocation_id": workflow_invocation_id,
            },
        )
