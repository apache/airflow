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

from airflow.configuration import conf
from airflow.exceptions import AirflowConfigException
from airflow.models.dagbundle import DagBundleModel
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.module_loading import import_string
from airflow.utils.session import NEW_SESSION, provide_session

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.dag_processing.bundles.base import BaseDagBundle


class DagBundlesManager(LoggingMixin):
    """Manager for DAG bundles."""

    @property
    def bundle_configs(self) -> dict[str, dict]:
        """Get all DAG bundle configurations."""
        configured_bundles = conf.getsection("dag_bundles")

        if not configured_bundles:
            return {}

        # If dags_folder is empty string, we remove it. This allows the default dags_folder bundle to be disabled.
        if not configured_bundles["dags_folder"]:
            del configured_bundles["dags_folder"]

        dict_bundles: dict[str, dict] = {}
        for key in configured_bundles.keys():
            config = conf.getjson("dag_bundles", key)
            if not isinstance(config, dict):
                raise AirflowConfigException(f"Bundle config for {key} is not a dict: {config}")
            dict_bundles[key] = config

        return dict_bundles

    @provide_session
    def sync_bundles_to_db(self, *, session: Session = NEW_SESSION) -> None:
        known_bundles = {b.name: b for b in session.query(DagBundleModel).all()}

        for name in self.bundle_configs.keys():
            if bundle := known_bundles.get(name):
                bundle.active = True
            else:
                session.add(DagBundleModel(name=name))
                self.log.info("Added new DAG bundle %s to the database", name)

        for name, bundle in known_bundles.items():
            if name not in self.bundle_configs:
                bundle.active = False
                self.log.warning("DAG bundle %s is no longer found in config and has been disabled", name)

    def get_all_dag_bundles(self) -> list[BaseDagBundle]:
        """
        Get all DAG bundles.

        :param session: A database session.

        :return: list of DAG bundles.
        """
        return [self.get_bundle(name, version=None) for name in self.bundle_configs.keys()]

    def get_bundle(self, name: str, version: str | None = None) -> BaseDagBundle:
        """
        Get a DAG bundle by name.

        :param name: The name of the DAG bundle.
        :param version: The version of the DAG bundle you need (optional). If not provided, ``tracking_ref`` will be used instead.

        :return: The DAG bundle.
        """
        # TODO: proper validation of the bundle configuration so we have better error messages
        bundle_config = self.bundle_configs[name]
        bundle_class = import_string(bundle_config["classpath"])
        return bundle_class(name=name, version=version, **bundle_config["kwargs"])
