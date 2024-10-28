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
from typing import TYPE_CHECKING, Union

import attr

from airflow.assets import Asset
from airflow.providers_manager import ProvidersManager
from airflow.utils.log.logging_mixin import LoggingMixin

if TYPE_CHECKING:
    from airflow.hooks.base import BaseHook
    from airflow.io.path import ObjectStoragePath

    # Store context what sent lineage.
    LineageContext = Union[BaseHook, ObjectStoragePath]

_hook_lineage_collector: HookLineageCollector | None = None


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
class HookLineage:
    """
    Holds lineage collected by HookLineageCollector.

    This class represents the lineage information collected by the `HookLineageCollector`. It stores
    the input and output assets, each with an associated count indicating how many times the asset
    has been encountered during the hook execution.
    """

    inputs: list[AssetLineageInfo] = attr.ib(factory=list)
    outputs: list[AssetLineageInfo] = attr.ib(factory=list)


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

    def _generate_key(self, asset: Asset, context: LineageContext) -> str:
        """
        Generate a unique key for the given asset and context.

        This method creates a unique key by combining the asset URI, the MD5 hash of the asset's extra
        dictionary, and the LineageContext's unique identifier. This ensures that the generated key is
        unique for each combination of asset and context.
        """
        extra_str = json.dumps(asset.extra, sort_keys=True)
        extra_hash = hashlib.md5(extra_str.encode()).hexdigest()
        return f"{asset.uri}_{extra_hash}_{id(context)}"

    def create_asset(
        self,
        scheme: str | None,
        uri: str | None,
        asset_kwargs: dict | None,
        asset_extra: dict | None,
    ) -> Asset | None:
        """
        Create an asset instance using the provided parameters.

        This method attempts to create an asset instance using the given parameters.
        It first checks if a URI is provided and falls back to using the default asset factory
        with the given URI if no other information is available.

        If a scheme is provided but no URI, it attempts to find an asset factory that matches
        the given scheme. If no such factory is found, it logs an error message and returns None.

        If asset_kwargs is provided, it is used to pass additional parameters to the asset
        factory. The asset_extra parameter is also passed to the factory as an ``extra`` parameter.
        """
        if uri:
            # Fallback to default factory using the provided URI
            return Asset(uri=uri, extra=asset_extra)

        if not scheme:
            self.log.debug(
                "Missing required parameter: either 'uri' or 'scheme' must be provided to create an asset."
            )
            return None

        asset_factory = ProvidersManager().asset_factories.get(scheme)
        if not asset_factory:
            self.log.debug(
                "Unsupported scheme: %s. Please provide a valid URI to create an asset.",
                scheme,
            )
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
        asset_kwargs: dict | None = None,
        asset_extra: dict | None = None,
    ):
        """Add the input asset and its corresponding hook execution context to the collector."""
        asset = self.create_asset(
            scheme=scheme, uri=uri, asset_kwargs=asset_kwargs, asset_extra=asset_extra
        )
        if asset:
            key = self._generate_key(asset, context)
            if key not in self._inputs:
                self._inputs[key] = (asset, context)
            self._input_counts[key] += 1

    def add_output_asset(
        self,
        context: LineageContext,
        scheme: str | None = None,
        uri: str | None = None,
        asset_kwargs: dict | None = None,
        asset_extra: dict | None = None,
    ):
        """Add the output asset and its corresponding hook execution context to the collector."""
        asset = self.create_asset(
            scheme=scheme, uri=uri, asset_kwargs=asset_kwargs, asset_extra=asset_extra
        )
        if asset:
            key = self._generate_key(asset, context)
            if key not in self._outputs:
                self._outputs[key] = (asset, context)
            self._output_counts[key] += 1

    @property
    def collected_assets(self) -> HookLineage:
        """Get the collected hook lineage information."""
        return HookLineage(
            [
                AssetLineageInfo(
                    asset=asset, count=self._input_counts[key], context=context
                )
                for key, (asset, context) in self._inputs.items()
            ],
            [
                AssetLineageInfo(
                    asset=asset, count=self._output_counts[key], context=context
                )
                for key, (asset, context) in self._outputs.items()
            ],
        )

    @property
    def has_collected(self) -> bool:
        """Check if any assets have been collected."""
        return len(self._inputs) != 0 or len(self._outputs) != 0


class NoOpCollector(HookLineageCollector):
    """
    NoOpCollector is a hook lineage collector that does nothing.

    It is used when you want to disable lineage collection.
    """

    def add_input_asset(self, *_, **__):
        pass

    def add_output_asset(self, *_, **__):
        pass

    @property
    def collected_assets(
        self,
    ) -> HookLineage:
        self.log.warning(
            "Data lineage tracking is disabled. Register a hook lineage reader to start tracking hook lineage."
        )
        return HookLineage([], [])


class HookLineageReader(LoggingMixin):
    """Class used to retrieve the hook lineage information collected by HookLineageCollector."""

    def __init__(self, **kwargs):
        self.lineage_collector = get_hook_lineage_collector()

    def retrieve_hook_lineage(self) -> HookLineage:
        """Retrieve hook lineage from HookLineageCollector."""
        hook_lineage = self.lineage_collector.collected_assets
        return hook_lineage


def get_hook_lineage_collector() -> HookLineageCollector:
    """Get singleton lineage collector."""
    global _hook_lineage_collector
    if not _hook_lineage_collector:
        from airflow import plugins_manager

        plugins_manager.initialize_hook_lineage_readers_plugins()
        if plugins_manager.hook_lineage_reader_classes:
            _hook_lineage_collector = HookLineageCollector()
        else:
            _hook_lineage_collector = NoOpCollector()
    return _hook_lineage_collector
