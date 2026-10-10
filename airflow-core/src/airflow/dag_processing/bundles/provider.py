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

from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import cache
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ValidationError

from airflow._shared.module_loading import import_string
from airflow.configuration import conf
from airflow.dag_processing.bundles.base import BaseDagBundle  # noqa: TC001
from airflow.exceptions import AirflowConfigException

if TYPE_CHECKING:
    from collections.abc import Sequence


@dataclass(frozen=True)
class DagBundleMetadata:
    """Metadata used by Airflow to manage a Dag bundle."""

    name: str
    team_name: str | None = None


class DagBundleProvider(ABC):
    """
    Provide configured Dag bundle metadata and construct Dag bundles.

    The metadata list is the complete set of configured bundles. A bundle name
    may remain resolvable through ``get_bundle`` after it leaves that list because
    retained Dag runs can still need an older version. A name must continue to
    identify the same bundle construction settings. Use a new name when settings
    such as the bundle class, repository, branch, connection, or refresh interval change.
    """

    @property
    def provides_complete_bundle_list(self) -> bool:
        """Return whether each provider instance sees the complete configured bundle list."""
        return True

    @abstractmethod
    def get_configured_bundle_metadata(self) -> Sequence[DagBundleMetadata]:
        """
        Return metadata for the complete set of configured Dag bundles.

        Return an empty sequence only when no bundles are configured. Raise an exception when
        the current metadata cannot be read so Airflow keeps the last valid bundle list.
        """

    @abstractmethod
    def get_bundle(
        self,
        name: str,
        version: str | None = None,
        version_data: dict[str, Any] | None = None,
    ) -> BaseDagBundle:
        """Return a Dag bundle by name and optional version information."""


class _ExternalBundleConfig(BaseModel):
    """Schema defining the user-specified configuration for a Dag bundle."""

    name: str
    classpath: str
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

        bundles[cfg.name] = cfg
    if len(bundles.keys()) != len(config_list):
        raise _bundle_item_exc("One or more bundle names appeared multiple times")
    return list(bundles.values())


@cache
def _load_bundle_config_snapshot() -> tuple[_ExternalBundleConfig, ...]:
    """Read the static bundle configuration without importing bundle classes."""
    config_list = conf.getjson("dag_processor", "dag_bundle_config_list")
    if not config_list:
        return ()
    if not isinstance(config_list, list):
        raise AirflowConfigException(
            "Section `dag_processor` key `dag_bundle_config_list` "
            f"must be list but got {config_list.__class__}"
        )
    return tuple(_parse_bundle_config(config_list))


class ConfigDagBundleProvider(DagBundleProvider):
    """Provide Dag bundles configured by ``dag_bundle_config_list``."""

    @property
    def provides_complete_bundle_list(self) -> bool:
        return False

    def __init__(self) -> None:
        self._bundle_config = {config.name: config for config in _load_bundle_config_snapshot()}
        self._bundle_classes: dict[str, type[BaseDagBundle]] = {}

    def get_configured_bundle_metadata(self) -> Sequence[DagBundleMetadata]:
        return [
            DagBundleMetadata(name=name, team_name=config.team_name)
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
        if name not in self._bundle_classes:
            self._bundle_classes[name] = import_string(cfg_bundle.classpath)
        return self._bundle_classes[name](
            name=name, version=version, version_data=version_data, **cfg_bundle.kwargs
        )
