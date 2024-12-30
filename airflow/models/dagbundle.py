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

from sqlalchemy import Boolean, Column, String

from airflow.models.base import Base, StringID
from airflow.utils.module_loading import import_string
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.sqlalchemy import UtcDateTime

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.dag_processing.bundles.base import BaseDagBundle


class DagBundleModel(Base):
    """
    A table for storing DAG bundle metadata.

    We track the following information about each bundle, as it can be useful for
    informational purposes and for debugging:
     - active: Is the bundle currently found in configuration?
     - latest_version: The latest version Airflow has seen for the bundle.
     - last_refreshed: When the bundle was last refreshed.
    """

    __tablename__ = "dag_bundle"
    name = Column(StringID(), primary_key=True)
    active = Column(Boolean, default=True)
    latest_version = Column(String(200), nullable=True)
    last_refreshed = Column(UtcDateTime, nullable=True)

    def __init__(self, *, name: str, latest_version=None, last_refreshed=None):
        self.name = name
        self.latest_version = latest_version
        self.last_refreshed = last_refreshed

    @classmethod
    @provide_session
    def get_all_dag_bundles(
        cls, *, session: Session = NEW_SESSION
    ) -> list[tuple[DagBundleModel, BaseDagBundle]]:
        """
        Get all DAG bundles.

        :param session: A database session.
        :return: list of DAG bundles.
        """
        bundle_configs = session.query(cls).all()

        bundles = []
        for bundle_config in bundle_configs:
            bundle_class = import_string(bundle_config.classpath)
            bundle = bundle_class(name=bundle_config.name, **bundle_config.kwargs)
            bundles.append((bundle_config, bundle))

        return bundles
