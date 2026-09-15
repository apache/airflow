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

import importlib
import logging
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ValidationError

from airflow._shared.module_loading import import_string
from airflow.configuration import conf
from airflow.dag_processing.bundles.base import BaseDagBundle  # noqa: TC001
from airflow.exceptions import AirflowConfigException
from airflow.providers_manager import ProvidersManager

if TYPE_CHECKING:
    from collections.abc import Sequence

log = logging.getLogger(__name__)

_example_dag_bundle_name = "example_dags"


@dataclass(frozen=True)
class DagBundleConfiguration:
    """Configuration used by Airflow to manage a Dag bundle."""

    name: str
    team_name: str | None = None


class DagBundleProvider(ABC):
    """
    Provide Dag bundle configurations and construct Dag bundles.

    The configuration list is the complete set of active bundles. A bundle name
    may remain resolvable through ``get_bundle`` after it leaves that list because
    retained Dag runs can still need an older version. A name must continue to
    identify the same bundle implementation; use a new name for a different one.
    """

    @abstractmethod
    def get_all_bundle_configurations(self) -> Sequence[DagBundleConfiguration]:
        """Return the complete set of active Dag bundle configurations."""

    @abstractmethod
    def get_bundle(
        self,
        name: str,
        version: str | None = None,
        version_data: dict[str, Any] | None = None,
    ) -> BaseDagBundle:
        """Construct a Dag bundle by name and optional version information."""


class _ExternalBundleConfig(BaseModel):
    """Schema defining the user-specified configuration for a Dag bundle."""

    name: str
    classpath: str
    kwargs: dict
    team_name: str | None = None


class _InternalBundleConfig(BaseModel):
    """
    Schema used internally (in this file) to define the configuration for a Dag bundle.

    Configuration defined by users when read must match ``_ExternalBundleConfig``.
    This configuration is then parsed and converted to ``_InternalBundleConfig`` to be used across this file.
    """

    bundle_class: type[BaseDagBundle]
    kwargs: dict
    team_name: str | None = None


def _bundle_item_exc(msg):
    return AirflowConfigException(
        "Invalid config for section `dag_processor` key `dag_bundle_config_list`. " + msg
    )


def _parse_bundle_config(config_list) -> list[_ExternalBundleConfig]:
    bundles = {}
    for item in config_list:
        if not isinstance(item, dict):
            raise _bundle_item_exc(f"Expected dict but got {item.__class__}")

        try:
            cfg = _ExternalBundleConfig(**item)
        except ValidationError as e:
            raise _bundle_item_exc(f"Item {item} failed validation: {e}")

        if cfg.name == _example_dag_bundle_name:
            raise AirflowConfigException(
                f"Bundle name '{_example_dag_bundle_name}' is a reserved name. Please choose another name for your bundle."
                " Example Dags can be enabled with the '[core] load_examples' config."
            )

        bundles[cfg.name] = cfg
    if len(bundles.keys()) != len(config_list):
        raise _bundle_item_exc("One or more bundle names appeared multiple times")
    return list(bundles.values())


def _add_example_dag_bundle(bundle_config_list: list[_ExternalBundleConfig]):
    from airflow import example_dags

    example_dag_folder = next(iter(example_dags.__path__))
    bundle_config_list.append(
        _ExternalBundleConfig(
            name=_example_dag_bundle_name,
            classpath="airflow.dag_processing.bundles.local.LocalDagBundle",
            kwargs={
                "path": example_dag_folder,
            },
        )
    )


def _add_provider_example_dags_to_bundle(bundle_config_list: list[_ExternalBundleConfig]):
    """
    Add an ``example_dags`` folder of every installed provider as a bundle.

    Provider locations are resolved through ``ProvidersManager`` instead of
    walking ``airflow.providers.__path__`` so that:

    - nested providers (e.g. ``apache-airflow-providers-common-sql`` whose
      module path is ``airflow.providers.common.sql``) are discovered;
    - providers installed outside the ``airflow.providers`` namespace package
      are discovered via their entry point.
    """
    # Dedup on the resolved on-disk folder rather than the bundle name: distributions
    # under ``airflow.providers.common.*`` use ``pkgutil.extend_path``, so when several
    # ``common-*`` packages are installed ``airflow.providers.common.__path__`` has
    # multiple entries and the inner loop iterates more than once. Path-based dedup
    # only skips when the same folder is seen twice; distinct folders are preserved.
    seen: set[str] = set()

    for package_name in ProvidersManager().providers:
        # Heuristic: derive the import path from the canonical
        # ``apache-airflow-providers-*`` distribution name. Tracked as a follow-up
        # to record the provider module path on ``ProviderInfo`` (see
        # https://github.com/apache/airflow/issues/66305).
        if package_name.startswith("apache-airflow-providers-"):
            suffix = package_name[len("apache-airflow-providers-") :]
            module_name = "airflow.providers." + suffix.replace("-", ".")
        else:
            module_name = package_name.replace("-", "_")
        try:
            module = importlib.import_module(module_name)
            module_paths = list(getattr(module, "__path__", []))
        except Exception:
            log.exception("Could not load provider module %s for example Dag discovery", module_name)
            continue

        for module_path in module_paths:
            example_dag_folder = os.path.join(module_path, "example_dags")
            if not os.path.isdir(example_dag_folder):
                continue
            if example_dag_folder in seen:
                continue
            seen.add(example_dag_folder)
            bundle_name = f"{package_name}-example-dags"
            bundle_config_list.append(
                _ExternalBundleConfig(
                    name=bundle_name,
                    classpath="airflow.dag_processing.bundles.local.LocalDagBundle",
                    kwargs={
                        "path": example_dag_folder,
                    },
                )
            )


class ConfigDagBundleProvider(DagBundleProvider):
    """Provide Dag bundles configured by ``dag_bundle_config_list``."""

    def __init__(self) -> None:
        self._bundle_config: dict[str, _InternalBundleConfig] = {}
        self.parse_config()

    def parse_config(self) -> None:
        """
        Get all Dag bundle configurations and store in instance variable.

        If a bundle class for a given name has already been imported, it will not be imported again.

        :meta private:
        """
        if self._bundle_config:
            return

        config_list = conf.getjson("dag_processor", "dag_bundle_config_list")
        if not config_list:
            return
        if not isinstance(config_list, list):
            raise AirflowConfigException(
                "Section `dag_processor` key `dag_bundle_config_list` "
                f"must be list but got {config_list.__class__}"
            )
        bundle_config_list = _parse_bundle_config(config_list)
        if conf.getboolean("core", "LOAD_EXAMPLES"):
            _add_example_dag_bundle(bundle_config_list)
            _add_provider_example_dags_to_bundle(bundle_config_list)

        for bundle_config in bundle_config_list:
            class_ = import_string(bundle_config.classpath)
            self._bundle_config[bundle_config.name] = _InternalBundleConfig(
                bundle_class=class_,
                kwargs=bundle_config.kwargs,
                team_name=bundle_config.team_name,
            )

    def get_all_bundle_configurations(self) -> Sequence[DagBundleConfiguration]:
        return [
            DagBundleConfiguration(name=name, team_name=config.team_name)
            for name, config in self._bundle_config.items()
        ]

    def get_bundle(
        self,
        name: str,
        version: str | None = None,
        version_data: dict[str, Any] | None = None,
    ) -> BaseDagBundle:
        cfg_bundle = self._bundle_config.get(name)
        if not cfg_bundle:
            raise ValueError(f"Requested bundle '{name}' is not configured.")
        return cfg_bundle.bundle_class(
            name=name, version=version, version_data=version_data, **cfg_bundle.kwargs
        )
