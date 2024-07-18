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

from typing import Union

import attr

from airflow.datasets import Dataset
from airflow.hooks.base import BaseHook
from airflow.io.store import ObjectStore
from airflow.providers_manager import ProvidersManager
from airflow.utils.log.logging_mixin import LoggingMixin

# Store context what sent lineage.
LineageContext = Union[BaseHook, ObjectStore]

_hook_lineage_collector: HookLineageCollector | None = None


@attr.define
class HookLineage:
    """Holds lineage collected by HookLineageCollector."""

    inputs: list[tuple[Dataset, LineageContext]] = attr.ib(factory=list)
    outputs: list[tuple[Dataset, LineageContext]] = attr.ib(factory=list)


class HookLineageCollector(LoggingMixin):
    """
    HookLineageCollector is a base class for collecting hook lineage information.

    It is used to collect the input and output datasets of a hook execution.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.inputs: list[tuple[Dataset, LineageContext]] = []
        self.outputs: list[tuple[Dataset, LineageContext]] = []

    def create_dataset(
        self, scheme: str | None, uri: str | None, dataset_kwargs: dict | None, dataset_extra: dict | None
    ) -> Dataset | None:
        """
        Create a Dataset instance using the provided parameters.

        This method attempts to create a Dataset instance using the given parameters.
        It first checks if a URI is provided and falls back to using the default dataset factory
        with the given URI if no other information is available.

        If a scheme is provided but no URI, it attempts to find a dataset factory that matches
        the given scheme. If no such factory is found, it logs an error message and returns None.

        If dataset_kwargs is provided, it is used to pass additional parameters to the Dataset
        factory. The dataset_extra parameter is also passed to the factory as an ``extra`` parameter.
        """
        if uri:
            # Fallback to default factory using the provided URI
            return Dataset(uri=uri, extra=dataset_extra)

        if not scheme:
            self.log.debug(
                "Missing required parameter: either 'uri' or 'scheme' must be provided to create a Dataset."
            )
            return None

        dataset_factory = ProvidersManager().dataset_factories.get(scheme)
        if not dataset_factory:
            self.log.debug("Unsupported scheme: %s. Please provide a valid URI to create a Dataset.", scheme)
            return None

        dataset_kwargs = dataset_kwargs or {}
        try:
            return dataset_factory(**dataset_kwargs, extra=dataset_extra)
        except Exception as e:
            self.log.debug("Failed to create dataset. Skipping. Error: %s", e)
            return None

    def add_input_dataset(
        self,
        context: LineageContext,
        scheme: str | None = None,
        uri: str | None = None,
        dataset_kwargs: dict | None = None,
        dataset_extra: dict | None = None,
    ):
        """Add the input dataset and its corresponding hook execution context to the collector."""
        dataset = self.create_dataset(
            scheme=scheme, uri=uri, dataset_kwargs=dataset_kwargs, dataset_extra=dataset_extra
        )
        if dataset:
            self.inputs.append((dataset, context))

    def add_output_dataset(
        self,
        context: LineageContext,
        scheme: str | None = None,
        uri: str | None = None,
        dataset_kwargs: dict | None = None,
        dataset_extra: dict | None = None,
    ):
        """Add the output dataset and its corresponding hook execution context to the collector."""
        dataset = self.create_dataset(
            scheme=scheme, uri=uri, dataset_kwargs=dataset_kwargs, dataset_extra=dataset_extra
        )
        if dataset:
            self.outputs.append((dataset, context))

    @property
    def collected_datasets(self) -> HookLineage:
        """Get the collected hook lineage information."""
        return HookLineage(self.inputs, self.outputs)

    @property
    def has_collected(self) -> bool:
        """Check if any datasets have been collected."""
        return len(self.inputs) != 0 or len(self.outputs) != 0


class NoOpCollector(HookLineageCollector):
    """
    NoOpCollector is a hook lineage collector that does nothing.

    It is used when you want to disable lineage collection.
    """

    def add_input_dataset(self, *_):
        pass

    def add_output_dataset(self, *_):
        pass

    @property
    def collected_datasets(
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
        hook_lineage = self.lineage_collector.collected_datasets
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
