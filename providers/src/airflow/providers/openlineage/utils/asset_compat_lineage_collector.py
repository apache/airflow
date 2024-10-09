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

from importlib.util import find_spec

# TODO: replace this module with common.compat provider once common.compat 1.3.0 released


def _get_asset_compat_hook_lineage_collector():
    from airflow.lineage.hook import get_hook_lineage_collector

    collector = get_hook_lineage_collector()

    if all(
        getattr(collector, asset_method_name, None)
        for asset_method_name in ("add_input_asset", "add_output_asset", "collected_assets")
    ):
        return collector

    # dataset is renamed as asset in Airflow 3.0

    from functools import wraps

    from airflow.lineage.hook import DatasetLineageInfo, HookLineage

    DatasetLineageInfo.asset = DatasetLineageInfo.dataset

    def rename_dataset_kwargs_as_assets_kwargs(function):
        @wraps(function)
        def wrapper(*args, **kwargs):
            if "asset_kwargs" in kwargs:
                kwargs["dataset_kwargs"] = kwargs.pop("asset_kwargs")

            if "asset_extra" in kwargs:
                kwargs["dataset_extra"] = kwargs.pop("asset_extra")

            return function(*args, **kwargs)

        return wrapper

    collector.create_asset = rename_dataset_kwargs_as_assets_kwargs(collector.create_dataset)
    collector.add_input_asset = rename_dataset_kwargs_as_assets_kwargs(collector.add_input_dataset)
    collector.add_output_asset = rename_dataset_kwargs_as_assets_kwargs(collector.add_output_dataset)

    def collected_assets_compat(collector) -> HookLineage:
        """Get the collected hook lineage information."""
        lineage = collector.collected_datasets
        return HookLineage(
            [
                DatasetLineageInfo(dataset=item.dataset, count=item.count, context=item.context)
                for item in lineage.inputs
            ],
            [
                DatasetLineageInfo(dataset=item.dataset, count=item.count, context=item.context)
                for item in lineage.outputs
            ],
        )

    setattr(
        collector.__class__,
        "collected_assets",
        property(lambda collector: collected_assets_compat(collector)),
    )

    return collector


def get_hook_lineage_collector():
    # HookLineageCollector added in 2.10
    try:
        if find_spec("airflow.assets"):
            # Dataset has been renamed as Asset in 3.0
            from airflow.lineage.hook import get_hook_lineage_collector

            return get_hook_lineage_collector()

        return _get_asset_compat_hook_lineage_collector()
    except ImportError:

        class NoOpCollector:
            """
            NoOpCollector is a hook lineage collector that does nothing.

            It is used when you want to disable lineage collection.
            """

            def add_input_asset(self, *_, **__):
                pass

            def add_output_asset(self, *_, **__):
                pass

        return NoOpCollector()
