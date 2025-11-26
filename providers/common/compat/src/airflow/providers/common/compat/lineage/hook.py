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

if TYPE_CHECKING:
    from typing import Any

    from airflow.lineage.hook import LineageContext


def _lacks_asset_methods(collector):
    """Return True if the collector is missing any asset-related methods or properties."""
    if (  # lazy evaluation, early return
        hasattr(collector, "add_input_asset")  # method
        and hasattr(collector, "add_output_asset")  # method
        and hasattr(collector, "create_asset")  # method
        # If below we called hasattr(collector, "collected_assets") we'd call the property unnecessarily
        and hasattr(type(collector), "collected_assets")  # property
    ):
        return False

    return True


def _lacks_add_extra_method(collector):
    """Return True if the collector does not define an 'add_extra' method."""
    # Method may be on class and attribute may be dynamically set on instance
    if hasattr(collector, "add_extra") and hasattr(collector, "_extra"):
        return False
    return True


def _add_extra_polyfill(collector):
    """
    Add support for `add_extra` method to collector that may be lacking it (e.g., Airflow versions < 3.2.).

    This polyfill adds the `add_extra` method to a class, modifies `collected_assets` and `has_collected`
    properties and sets `_extra` and `_extra_counts` attributes on instance if not already there.

    This function should be called after renaming on collectors that have `collected_assets` method,
    so f.e. for Airflow 2 it should happen after renaming from dataset to asset.
    """
    import hashlib
    import json
    from collections import defaultdict

    import attr

    from airflow.lineage.hook import HookLineage as _BaseHookLineage

    # Add `extra` to HookLineage returned by `collected_assets` property
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
    class HookLineage(_BaseHookLineage):
        # mypy is not happy, as base class is using other ExtraLineageInfo, but this code will never
        # run on AF3.2, where this other one is used, so this is fine - we can ignore.
        extra: list[ExtraLineageInfo] = attr.field(factory=list)  # type: ignore[assignment]

    # Initialize extra tracking attributes on this collector instance
    collector._extra = {}
    collector._extra_counts = defaultdict(int)

    # Overwrite the `collected_assets` property on a class
    _original_collected_assets = collector.__class__.collected_assets

    def _compat_collected_assets(self) -> HookLineage:
        """Get the collected hook lineage information."""
        # Defensive check since we patch the class property, but initialized _extra only on this instance.
        if not hasattr(self, "_extra"):
            self._extra = {}
        if not hasattr(self, "_extra_counts"):
            self._extra_counts = defaultdict(int)

        # call the original `collected_assets` getter
        lineage = _original_collected_assets.fget(self)
        extra_list = [
            ExtraLineageInfo(
                key=key,
                value=value,
                count=self._extra_counts[count_key],
                context=context,
            )
            for count_key, (key, value, context) in self._extra.items()
        ]
        return HookLineage(
            inputs=lineage.inputs,
            outputs=lineage.outputs,
            extra=extra_list,
        )

    type(collector).collected_assets = property(_compat_collected_assets)

    # Overwrite the `has_collected` property on a class
    _original_has_collected = collector.__class__.has_collected

    def _compat_has_collected(self) -> bool:
        # Defensive check since we patch the class property, but initialized _extra only on this instance.
        if not hasattr(self, "_extra"):
            self._extra = {}
        # call the original `has_collected` getter
        has_collected = _original_has_collected.fget(self)
        return bool(has_collected or self._extra)

    type(collector).has_collected = property(_compat_has_collected)

    # Add `add_extra` method on the class
    def _compat_add_extra(self, context, key, value):
        """Add extra information for older Airflow versions."""
        _max_collected_extra = 200

        if len(self._extra) >= _max_collected_extra:
            if hasattr(self, "log"):
                self.log.debug("Maximum number of extra exceeded. Skipping.")
            return

        if not key or not value:
            if hasattr(self, "log"):
                self.log.debug("Missing required parameter: both 'key' and 'value' must be provided.")
            return

        # Defensive check since we patch the class property, but initialized _extra only on this instance.
        if not hasattr(self, "_extra"):
            self._extra = {}
        if not hasattr(self, "_extra_counts"):
            self._extra_counts = defaultdict(int)

        extra_str = json.dumps(value, sort_keys=True, default=str)
        value_hash = hashlib.md5(extra_str.encode()).hexdigest()
        entry_id = f"{key}_{value_hash}_{id(context)}"
        if entry_id not in self._extra:
            self._extra[entry_id] = (key, value, context)
        self._extra_counts[entry_id] += 1

        if len(self._extra) == _max_collected_extra:
            if hasattr(self, "log"):
                self.log.warning("Maximum number of extra exceeded. Skipping subsequent inputs.")

    type(collector).add_extra = _compat_add_extra
    return collector


def _add_asset_naming_compatibility_layer(collector):
    """
    Handle AF 2.x compatibility for dataset -> asset terminology rename.

    This is only called for AF 2.x where we need to provide asset-named methods
    that wrap the underlying dataset methods.
    """
    from functools import wraps

    from airflow.lineage.hook import DatasetLineageInfo, HookLineage

    DatasetLineageInfo.asset = DatasetLineageInfo.dataset

    def rename_asset_kwargs_to_dataset_kwargs(function):
        @wraps(function)
        def wrapper(*args, **kwargs):
            if "asset_kwargs" in kwargs:
                kwargs["dataset_kwargs"] = kwargs.pop("asset_kwargs")

            if "asset_extra" in kwargs:
                kwargs["dataset_extra"] = kwargs.pop("asset_extra")

            return function(*args, **kwargs)

        return wrapper

    collector.create_asset = rename_asset_kwargs_to_dataset_kwargs(collector.create_dataset)
    collector.add_input_asset = rename_asset_kwargs_to_dataset_kwargs(collector.add_input_dataset)
    collector.add_output_asset = rename_asset_kwargs_to_dataset_kwargs(collector.add_output_dataset)

    def _compat_collected_assets(self) -> HookLineage:
        """Get the collected hook lineage information."""
        lineage = self.collected_datasets
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

    type(collector).collected_assets = property(_compat_collected_assets)
    return collector


def get_hook_lineage_collector():
    """
    Return a hook lineage collector with all required compatibility layers applied.

    Compatibility is determined by inspecting the collector's available methods and
    properties (duck typing), rather than relying on the Airflow version number.

    Behavior by example:
    Airflow 2: Collector is missing asset-based methods and `add_extra` - apply both layers.
    Airflow 3.0–3.1: Collector has asset-based methods but lacks `add_extra` - apply single layer.
    Airflow 3.2+: Collector has asset-based methods and `add_extra` support - no action required.
    """
    from airflow.lineage.hook import get_hook_lineage_collector as get_global_collector

    global_collector = get_global_collector()

    if _lacks_asset_methods(global_collector):
        global_collector = _add_asset_naming_compatibility_layer(global_collector)

    if _lacks_add_extra_method(global_collector):
        global_collector = _add_extra_polyfill(global_collector)

    return global_collector
