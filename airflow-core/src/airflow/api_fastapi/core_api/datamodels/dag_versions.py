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

from datetime import datetime
from uuid import UUID

from pydantic import AliasPath, Field, computed_field
from sqlalchemy import select

from airflow.api_fastapi.core_api.base import BaseModel
from airflow.dag_processing.bundles.manager import DagBundlesManager


class DagVersionResponse(BaseModel):
    """Dag Version serializer for responses."""

    id: UUID
    version_number: int
    dag_id: str
    bundle_name: str | None
    bundle_version: str | None
    created_at: datetime
    dag_display_name: str = Field(validation_alias=AliasPath("dag_model", "dag_display_name"))

    # Mypy issue https://github.com/python/mypy/issues/1362
    @computed_field  # type: ignore[prop-decorator]
    @property
    def bundle_url(self) -> str | None:
        if self.bundle_name:
            # Get the bundle model from the database and render the URL
            from airflow.models.dagbundle import DagBundleModel
            from airflow.utils.session import create_session

            with create_session() as session:
                bundle_model = session.scalar(
                    select(DagBundleModel).where(DagBundleModel.name == self.bundle_name)
                )

                if bundle_model and hasattr(bundle_model, "signed_url_template"):
                    return bundle_model.render_url(self.bundle_version)
                # fallback to the deprecated option if the bundle model does not have a signed_url_template
                # attribute
                try:
                    return DagBundlesManager().view_url(self.bundle_name, self.bundle_version)
                except ValueError:
                    return None
        return None


class DAGVersionCollectionResponse(BaseModel):
    """DAG Version Collection serializer for responses."""

    dag_versions: list[DagVersionResponse]
    total_entries: int
