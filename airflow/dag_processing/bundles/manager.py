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

from collections.abc import Iterable
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

_bundle_config = {}


class DagBundlesManager(LoggingMixin):
    """Manager for DAG bundles."""

    def parse_config(self) -> None:
        """
        Get all DAG bundle configurations and store in module variable.

        If a bundle class for a given name has already been imported, it will not be imported again.
        """
        configured_bundles = conf.getjson("dag_bundles", "backends")

        if not configured_bundles:
            return

        if not isinstance(configured_bundles, list):
            raise AirflowConfigException(
                "Bundle config is not a list. Check config value"
                " for section `dag_bundles` and key `backends`."
            )
        seen = set()
        for cfg in configured_bundles:
            name = cfg["name"]
            if name in seen:
                raise ValueError(f"Dag bundle {name} is configured twice.")
            seen.add(name)
            if name in _bundle_config:
                continue
            class_ = import_string(cfg["classpath"])
            kwargs = cfg["kwargs"]
            _bundle_config[name] = (class_, kwargs)

        # remove obsolete bundle configs
        for name in list(_bundle_config.keys()):
            if name not in seen:
                _bundle_config.pop(name)

    @provide_session
    def sync_bundles_to_db(self, *, session: Session = NEW_SESSION) -> None:
        self.parse_config()
        stored = {b.name: b for b in session.query(DagBundleModel).all()}
        for name in _bundle_config.keys():
            if bundle := stored.pop(name, None):
                bundle.active = True
            else:
                session.add(DagBundleModel(name=name))
                self.log.info("Added new DAG bundle %s to the database", name)

        for name, bundle in stored.items():
            bundle.active = False
            self.log.warning("DAG bundle %s is no longer found in config and has been disabled", name)

    def get_bundle(self, name: str, version: str | None = None) -> BaseDagBundle:
        """
        Get a DAG bundle by name.

        :param name: The name of the DAG bundle.
        :param version: The version of the DAG bundle you need (optional). If not provided, ``tracking_ref`` will be used instead.

        :return: The DAG bundle.
        """
        # todo (AIP-66): proper validation of the bundle configuration so we have better error messages
        cfg_tuple = _bundle_config.get(name)
        if not cfg_tuple:
            self.parse_config()
            cfg_tuple = _bundle_config.get(name)
            if not cfg_tuple:
                raise ValueError(f"Requested bundle '{name}' is not configured.")
        class_, kwargs = cfg_tuple
        return class_(name=name, version=version, **kwargs)

    def get_all_dag_bundles(self) -> Iterable[BaseDagBundle]:
        """
        Get all DAG bundles.

        :return: list of DAG bundles.
        """
        self.parse_config()
        for name, (class_, kwargs) in _bundle_config.items():
            yield class_(name=name, version=None, **kwargs)
