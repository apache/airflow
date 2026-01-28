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
from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from functools import cache
from typing import TYPE_CHECKING, Any, TypeAlias

import attr

from airflow.providers_manager import ProvidersManager
from airflow.sdk.definitions.asset import Asset
from airflow.utils.log.logging_mixin import LoggingMixin

if TYPE_CHECKING:
    from pydantic.types import JsonValue

    from airflow.sdk import BaseHook, ObjectStoragePath

    # Store context what sent lineage.
    LineageContext: TypeAlias = BaseHook | ObjectStoragePath


# Maximum number of assets input or output that can be collected in a single hook execution.
# Input assets and output assets are collected separately.
MAX_COLLECTED_ASSETS = 100

# Maximum number of extra metadata that can be collected in a single hook execution.
MAX_COLLECTED_EXTRA = 200


@attr.define
class AssetLineageInfo:
    """
    Holds lineage information for a single asset.

    This class represents the lineage information for a single asset, including the asset itself,
    the count of how many times it has been encountered, and the context in which it was encountered.
    """

    asset: Asset
    count: int
    context: LineageContext


@attr.define
class ExtraLineageInfo:
    """
    Holds lineage information for arbitrary non-asset metadata.

    This class represents additional lineage context captured during a hook execution that is not
    associated with a specific asset. It includes the metadata payload itself, the count of
    how many times it has been encountered, and the context in which it was encountered.
    """

    key: str
    value: Any
    count: int
    context: LineageContext


@attr.define
class HookLineage:
    """
    Holds lineage collected by HookLineageCollector.

    This class represents the lineage information collected by the `HookLineageCollector`. It stores
    the input and output assets, each with an associated count indicating how many times the asset
    has been encountered during the hook execution.
    """

    inputs: list[AssetLineageInfo] = attr.ib(factory=list)
    outputs: list[AssetLineageInfo] = attr.ib(factory=list)
    extra: list[ExtraLineageInfo] = attr.ib(factory=list)


class HookLineageCollector(LoggingMixin):
    """
    HookLineageCollector is a base class for collecting hook lineage information.

    It is used to collect the input and output assets of a hook execution.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # Dictionary to store input assets, counted by unique key (asset URI, MD5 hash of extra
        # dictionary, and LineageContext's unique identifier)
        self._inputs: dict[str, tuple[Asset, LineageContext]] = {}
        self._outputs: dict[str, tuple[Asset, LineageContext]] = {}
        self._input_counts: dict[str, int] = defaultdict(int)
        self._output_counts: dict[str, int] = defaultdict(int)
        self._asset_factories = ProvidersManager().asset_factories
        self._extra_counts: dict[str, int] = defaultdict(int)
        self._extra: dict[str, tuple[str, Any, LineageContext]] = {}

    @staticmethod
    def _generate_hash(value: Any) -> str:
        """
        Generate a deterministic MD5 hash for the given value.

        If the value is dictionary it's JSON-serialized with `sort_keys=True`, and unsupported types
        are converted to strings (`default=str`) to favor producing a hash rather than raising an error,
        even if that means a less precise encoding.
        """
        value_str = json.dumps(value, sort_keys=True, default=str)
        value_hash = hashlib.md5(value_str.encode()).hexdigest()
        return value_hash

    def _generate_asset_entry_id(self, asset: Asset, context: LineageContext) -> str:
        """
        Generate a unique key for the given asset and context.

        This method creates a unique key by combining the asset URI, the MD5 hash of the asset's extra
        dictionary, and the LineageContext's unique identifier. This ensures that the generated entry_id is
        unique for each combination of asset and context.
        """
        extra_hash = self._generate_hash(value=asset.extra)
        return f"{asset.uri}_{extra_hash}_{id(context)}"

    def _generate_extra_entry_id(self, key: str, value: Any, context: LineageContext) -> str:
        """
        Generate a unique key for the given extra lineage information and context.

        This method creates a unique key by combining the extra information key, an MD5 hash of the value,
        and the LineageContext's unique identifier. This ensures that the generated entry_id is unique
        for each combination of extra lineage information and context.
        """
        value_hash = self._generate_hash(value=value)
        return f"{key}_{value_hash}_{id(context)}"

    def create_asset(
        self,
        *,
        scheme: str | None = None,
        uri: str | None = None,
        name: str | None = None,
        group: str | None = None,
        asset_kwargs: dict | None = None,
        asset_extra: dict[str, JsonValue] | None = None,
    ) -> Asset | None:
        """
        Create an asset instance using the provided parameters.

        This method attempts to create an asset instance using the given parameters.
        It first checks if a URI or a name is provided and falls back to using the default asset factory
        with the given URI or name if no other information is available.

        If a scheme is provided but no URI or name, it attempts to find an asset factory that matches
        the given scheme. If no such factory is found, it logs an error message and returns None.

        If asset_kwargs is provided, it is used to pass additional parameters to the asset
        factory. The asset_extra parameter is also passed to the factory as an ``extra`` parameter.
        """
        if uri or name:
            # Fallback to default factory using the provided URI
            kwargs: dict[str, str | dict] = {}
            if uri:
                kwargs["uri"] = uri
            if name:
                kwargs["name"] = name
            if group:
                kwargs["group"] = group
            if asset_extra:
                kwargs["extra"] = asset_extra
            return Asset(**kwargs)  # type: ignore[call-overload]

        if not scheme:
            self.log.debug(
                "Missing required parameter: either 'uri' or 'scheme' must be provided to create an asset."
            )
            return None

        asset_factory = self._asset_factories.get(scheme)
        if not asset_factory:
            self.log.debug("Unsupported scheme: %s. Please provide a valid URI to create an asset.", scheme)
            return None

        asset_kwargs = asset_kwargs or {}
        try:
            return asset_factory(**asset_kwargs, extra=asset_extra)
        except Exception as e:
            self.log.debug("Failed to create asset. Skipping. Error: %s", e)
            return None

    def add_input_asset(
        self,
        context: LineageContext,
        scheme: str | None = None,
        uri: str | None = None,
        name: str | None = None,
        group: str | None = None,
        asset_kwargs: dict | None = None,
        asset_extra: dict[str, JsonValue] | None = None,
    ):
        """Add the input asset and its corresponding hook execution context to the collector."""
        if len(self._inputs) >= MAX_COLLECTED_ASSETS:
            self.log.debug("Maximum number of asset inputs exceeded. Skipping.")
            return
        asset = self.create_asset(
            scheme=scheme, uri=uri, name=name, group=group, asset_kwargs=asset_kwargs, asset_extra=asset_extra
        )
        if asset:
            entry_id = self._generate_asset_entry_id(asset, context)
            if entry_id not in self._inputs:
                self._inputs[entry_id] = (asset, context)
            self._input_counts[entry_id] += 1
        if len(self._inputs) == MAX_COLLECTED_ASSETS:
            self.log.warning("Maximum number of asset inputs exceeded. Skipping subsequent inputs.")

    def add_output_asset(
        self,
        context: LineageContext,
        scheme: str | None = None,
        uri: str | None = None,
        name: str | None = None,
        group: str | None = None,
        asset_kwargs: dict | None = None,
        asset_extra: dict[str, JsonValue] | None = None,
    ):
        """Add the output asset and its corresponding hook execution context to the collector."""
        if len(self._outputs) >= MAX_COLLECTED_ASSETS:
            self.log.debug("Maximum number of asset outputs exceeded. Skipping.")
            return
        asset = self.create_asset(
            scheme=scheme, uri=uri, name=name, group=group, asset_kwargs=asset_kwargs, asset_extra=asset_extra
        )
        if asset:
            entry_id = self._generate_asset_entry_id(asset=asset, context=context)
            if entry_id not in self._outputs:
                self._outputs[entry_id] = (asset, context)
            self._output_counts[entry_id] += 1
        if len(self._outputs) == MAX_COLLECTED_ASSETS:
            self.log.warning("Maximum number of asset outputs exceeded. Skipping subsequent inputs.")

    def add_extra(
        self,
        context: LineageContext,
        key: str,
        value: Any,
    ):
        """Add the extra information and its corresponding hook execution context to the collector."""
        if len(self._extra) >= MAX_COLLECTED_EXTRA:
            self.log.debug("Maximum number of extra exceeded. Skipping.")
            return
        if not key or not value:
            self.log.debug("Missing required parameter: both 'key' and 'value' must be provided.")
            return
        entry_id = self._generate_extra_entry_id(key=key, value=value, context=context)
        if entry_id not in self._extra:
            self._extra[entry_id] = (key, value, context)
        self._extra_counts[entry_id] += 1
        if len(self._extra) == MAX_COLLECTED_EXTRA:
            self.log.warning("Maximum number of extra exceeded. Skipping subsequent inputs.")

    @property
    def collected_assets(self) -> HookLineage:
        """Get the collected hook lineage information."""
        return HookLineage(
            inputs=[
                AssetLineageInfo(asset=asset, count=self._input_counts[key], context=context)
                for key, (asset, context) in self._inputs.items()
            ],
            outputs=[
                AssetLineageInfo(asset=asset, count=self._output_counts[key], context=context)
                for key, (asset, context) in self._outputs.items()
            ],
            extra=[
                ExtraLineageInfo(key=key, value=value, count=self._extra_counts[count_key], context=context)
                for count_key, (key, value, context) in self._extra.items()
            ],
        )

    @property
    def has_collected(self) -> bool:
        """Check if any assets have been collected."""
        return bool(self._inputs or self._outputs or self._extra)


class NoOpCollector(HookLineageCollector):
    """
    NoOpCollector is a hook lineage collector that does nothing.

    It is used when you want to disable lineage collection.
    """

    def add_input_asset(self, *_, **__):
        pass

    def add_output_asset(self, *_, **__):
        pass

    def add_extra(self, *_, **__):
        pass

    @property
    def collected_assets(
        self,
    ) -> HookLineage:
        self.log.debug(
            "Data lineage tracking is disabled. Register a hook lineage reader to start tracking hook lineage."
        )
        return HookLineage([], [], [])


class HookLineageReader(LoggingMixin):
    """Class used to retrieve the hook lineage information collected by HookLineageCollector."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.lineage_collector = get_hook_lineage_collector()

    def retrieve_hook_lineage(self) -> HookLineage:
        """Retrieve hook lineage from HookLineageCollector."""
        hook_lineage = self.lineage_collector.collected_assets
        return hook_lineage


@cache
def get_hook_lineage_collector() -> HookLineageCollector:
    """Get singleton lineage collector."""
    from airflow import plugins_manager

    if plugins_manager.get_hook_lineage_readers_plugins():
        return HookLineageCollector()
    return NoOpCollector()
