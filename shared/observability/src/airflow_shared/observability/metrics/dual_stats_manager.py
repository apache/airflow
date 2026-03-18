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

from contextlib import AbstractContextManager, ExitStack, nullcontext
from typing import TYPE_CHECKING, Any, ClassVar, cast

from .metrics_registry import MetricsRegistry
from .stats import Stats

if TYPE_CHECKING:
    from .protocols import DeltaType


def _value_is_provided(value: Any):
    """Return true if the value is not None and, if it has length > 0."""
    if value is None:
        return False
    try:
        # False for empty dicts and strings.
        return len(value) > 0
    except TypeError:
        # Numbers and bools that don't have `len`.
        return True


def _get_dict_with_defined_args(
    prov_count: int | None = None,
    prov_rate: int | float | None = None,
    prov_delta: bool | None = None,
    prov_tags: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Create a dict that will include only the parameters that have been provided."""
    defined_args_dict: dict[str, Any] = {}

    if _value_is_provided(prov_count):
        defined_args_dict["count"] = prov_count
    if _value_is_provided(prov_rate):
        defined_args_dict["rate"] = prov_rate
    if _value_is_provided(prov_delta):
        defined_args_dict["delta"] = prov_delta
    if _value_is_provided(prov_tags):
        defined_args_dict["tags"] = prov_tags

    return defined_args_dict


def _get_args_dict_with_extra_tags_if_set(
    args_dict: dict[str, Any] | None = None,
    prov_tags: dict[str, Any] | None = None,
    prov_tags_extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Create a new merged tags dict if there are extra tags.

    The new merged tags dict will replace the existing one, in the args dict.
    """
    # The args_dict already has the base tags.
    # If there are no `extra_tags`, this method is basically
    # returning the `args_dict` unchanged.
    args_dict_full = dict(args_dict) if args_dict is not None else {}

    tags_full = _get_tags_with_extra(prov_tags, prov_tags_extra)

    # Set `tags` only if there's something in `tags_full`.
    # If it's empty, remove any inherited key.
    if tags_full:
        args_dict_full["tags"] = tags_full
    else:
        args_dict_full.pop("tags", None)

    return args_dict_full


def _get_tags_with_extra(
    prov_tags: dict[str, Any] | None = None,
    prov_tags_extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Return a new dict with all tags if extra have been provided."""
    # If there are no extra tags then return the original tags.
    tags_full: dict[str, Any] = {}
    if prov_tags:
        tags_full.update(prov_tags)

    # If there are `extra_tags`, then add them to the dict.
    if prov_tags_extra is not None:
        tags_full.update(prov_tags_extra)

    return tags_full


class DualStatsManager:
    """Helper class to abstract enabling/disabling the export of metrics with legacy names."""

    metrics_dict = MetricsRegistry()

    # 'True' is also the default on the config.
    export_legacy_names: ClassVar[bool] = True

    @classmethod
    def initialize(cls, export_legacy_names: bool):
        cls.export_legacy_names = export_legacy_names

    @classmethod
    def get_legacy_stat(cls, stat: str, variables: dict[str, Any]) -> str | None:
        stat_from_registry = cls.metrics_dict.get(name=stat)

        # Validation 1: The metric must exist in the registry.
        if not stat_from_registry:
            raise ValueError(
                f"Metric '{stat}' not found in the registry. Add the metric to the YAML file before using it."
            )

        legacy_name = stat_from_registry.get("legacy_name", "-")

        if legacy_name == "-":
            return None

        # Get the required variables for the legacy name.
        required_vars = stat_from_registry.get("name_variables", [])

        # Validation 2: There must be a value for all required variables.
        missing_vars = set(required_vars) - set(variables.keys())
        if missing_vars:
            raise ValueError(
                f"Missing required variables for metric '{stat}': {sorted(missing_vars)}. "
                f"Required variables found in the registry: {required_vars}. "
                f"Provided variables: {sorted(variables.keys())}. "
                f"Provide all required variables."
            )

        # Extract only the variables needed for the legacy name.
        legacy_vars = {k: variables[k] for k in required_vars if k in variables}

        # Format and return the legacy name.
        return legacy_name.format(**legacy_vars)

    @classmethod
    def incr(
        cls,
        stat: str,
        count: int | None = None,
        rate: int | float | None = None,
        *,
        tags: dict[str, Any] | None = None,
        extra_tags: dict[str, Any] | None = None,
    ) -> None:
        kw = _get_dict_with_defined_args(count, rate, None, tags)

        if cls.export_legacy_names and extra_tags is not None:
            legacy_stat = cls.get_legacy_stat(stat=stat, variables=extra_tags)

            if legacy_stat is not None:
                # Emit legacy metric
                Stats.incr(legacy_stat, **kw)
            else:
                raise ValueError(f"Stat '{stat}' doesn't have a legacy name registered in the YAML file.")

        kw_with_extra_tags_if_set = _get_args_dict_with_extra_tags_if_set(kw, tags, extra_tags)
        Stats.incr(stat, **kw_with_extra_tags_if_set)

    @classmethod
    def decr(
        cls,
        stat: str,
        count: int | None = None,
        rate: int | float | None = None,
        *,
        tags: dict[str, Any] | None = None,
        extra_tags: dict[str, Any] | None = None,
    ) -> None:
        kw = _get_dict_with_defined_args(count, rate, None, tags)

        if cls.export_legacy_names and extra_tags is not None:
            legacy_stat = cls.get_legacy_stat(stat=stat, variables=extra_tags)

            if legacy_stat is not None:
                # Emit legacy metric
                Stats.decr(legacy_stat, **kw)
            else:
                raise ValueError(f"Stat '{stat}' doesn't have a legacy name registered in the YAML file.")

        kw_with_extra_tags_if_set = _get_args_dict_with_extra_tags_if_set(kw, tags, extra_tags)
        Stats.decr(stat, **kw_with_extra_tags_if_set)

    @classmethod
    def gauge(
        cls,
        stat: str,
        value: float,
        rate: int | float | None = None,
        delta: bool | None = None,
        *,
        tags: dict[str, Any] | None = None,
        extra_tags: dict[str, Any] | None = None,
    ) -> None:
        kw = _get_dict_with_defined_args(None, rate, delta, tags)

        if cls.export_legacy_names and extra_tags is not None:
            legacy_stat = cls.get_legacy_stat(stat=stat, variables=extra_tags)

            if legacy_stat is not None:
                # Emit legacy metric
                Stats.gauge(legacy_stat, value, **kw)
            else:
                raise ValueError(f"Stat '{stat}' doesn't have a legacy name registered in the YAML file.")

        kw_with_extra_tags_if_set = _get_args_dict_with_extra_tags_if_set(kw, tags, extra_tags)
        Stats.gauge(stat, value, **kw_with_extra_tags_if_set)

    @classmethod
    def timing(
        cls,
        stat: str,
        dt: DeltaType,
        *,
        tags: dict[str, Any] | None = None,
        extra_tags: dict[str, Any] | None = None,
    ) -> None:
        if cls.export_legacy_names and extra_tags is not None:
            legacy_stat = cls.get_legacy_stat(stat=stat, variables=extra_tags)

            if legacy_stat is not None:
                if tags:
                    Stats.timing(legacy_stat, dt, tags=tags)
                else:
                    Stats.timing(legacy_stat, dt)
            else:
                raise ValueError(f"Stat '{stat}' doesn't have a legacy name registered in the YAML file.")

        tags_with_extra = _get_tags_with_extra(tags, extra_tags)

        if tags_with_extra:
            Stats.timing(stat, dt, tags=tags_with_extra)
        else:
            Stats.timing(stat, dt)

    @classmethod
    def timer(
        cls,
        stat: str,
        tags: dict[str, Any] | None = None,
        extra_tags: dict[str, Any] | None = None,
        **kwargs,
    ):
        kw = dict(kwargs)
        if tags is not None:
            kw["tags"] = tags

        # Used with a context manager.
        stack = ExitStack()

        if cls.export_legacy_names and extra_tags is not None:
            legacy_stat = cls.get_legacy_stat(stat=stat, variables=extra_tags)

            ctx_mg1: AbstractContextManager[Any] = cast(
                "AbstractContextManager[Any]", Stats.timer(legacy_stat, **kw)
            )
        else:
            ctx_mg1 = nullcontext()

        stack.enter_context(ctx_mg1)

        kw_with_extra_tags_if_set = _get_args_dict_with_extra_tags_if_set(kw, tags, extra_tags)
        stack.enter_context(Stats.timer(stat, **kw_with_extra_tags_if_set))

        return stack
