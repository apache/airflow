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

# Only characters in the character set are considered valid
# for the stat_name if stat_name_default_handler is used.
from __future__ import annotations

import abc
import logging
import re
import string
import warnings
from collections.abc import Callable, Iterable
from functools import partial, wraps
from re import Pattern
from typing import cast

from airflow.configuration import conf

try:
    from airflow_shared.observability.exceptions import InvalidStatsNameException
except ModuleNotFoundError:
    from airflow._shared.observability.exceptions import InvalidStatsNameException

log = logging.getLogger(__name__)


class MetricNameLengthExemptionWarning(Warning):
    """
    A Warning class to be used for the metric name length exemption notice.

    Using a custom Warning class allows us to easily test that it is used.
    """


# Only characters in the character set are considered valid
# for the stat_name if stat_name_default_handler is used.
ALLOWED_CHARACTERS = frozenset(string.ascii_letters + string.digits + "_.-/")

# The following set contains existing metrics whose names are too long for
# OpenTelemetry and should be deprecated over time. This is implemented to
# ensure that any new metrics we introduce have names which meet the OTel
# standard while also allowing us time to deprecate the old names.
# NOTE:  No new names should be added to this list.  This list should
#        only ever shorten over time as we deprecate these names.
BACK_COMPAT_METRIC_NAME_PATTERNS: set[str] = {
    r"^(?P<job_name>.*)_start$",
    r"^(?P<job_name>.*)_end$",
    r"^(?P<job_name>.*)_heartbeat_failure$",
    r"^local_task_job.task_exit\.(?P<job_id>.*)\.(?P<dag_id>.*)\.(?P<task_id>.*)\.(?P<return_code>.*)$",
    r"^operator_failures_(?P<operator_name>.*)$",
    r"^operator_successes_(?P<operator_name>.*)$",
    r"^ti.start.(?P<dag_id>.*)\.(?P<task_id>.*)$",
    r"^ti.finish.(?P<dag_id>.*)\.(?P<task_id>.*)\.(?P<state>.*)$",
    r"^task_removed_from_dag\.(?P<dag_id>.*)$",
    r"^task_restored_to_dag\.(?P<dag_id>.*)$",
    r"^task_instance_created_(?P<operator_name>.*)$",
    r"^dag_processing\.last_run\.seconds_ago\.(?P<dag_file>.*)$",
    r"^pool\.open_slots\.(?P<pool_name>.*)$",
    r"^pool\.queued_slots\.(?P<pool_name>.*)$",
    r"^pool\.running_slots\.(?P<pool_name>.*)$",
    r"^pool\.deferred_slots\.(?P<pool_name>.*)$",
    r"^pool\.starving_tasks\.(?P<pool_name>.*)$",
    r"^dagrun\.dependency-check\.(?P<dag_id>.*)$",
    r"^dag\.(?P<dag_id>.*)\.(?P<task_id>.*)\.duration$",
    r"^dag\.(?P<dag_id>.*)\.(?P<task_id>.*)\.queued_duration$",
    r"^dag\.(?P<dag_id>.*)\.(?P<task_id>.*)\.scheduled_duration$",
    r"^dag_processing\.last_duration\.(?P<dag_file>.*)$",
    r"^dagrun\.duration\.success\.(?P<dag_id>.*)$",
    r"^dagrun\.duration\.failed\.(?P<dag_id>.*)$",
    r"^dagrun\.schedule_delay\.(?P<dag_id>.*)$",
    r"^dagrun\.(?P<dag_id>.*)\.first_task_scheduling_delay$",
}
BACK_COMPAT_METRIC_NAMES: set[Pattern[str]] = {re.compile(name) for name in BACK_COMPAT_METRIC_NAME_PATTERNS}

OTEL_NAME_MAX_LENGTH = 255
DEFAULT_VALIDATOR_TYPE = "allow"


def get_validator() -> ListValidator:
    validators = {
        "allow": PatternAllowListValidator,
        "block": PatternBlockListValidator,
    }
    metric_lists = {
        "allow": (metric_allow_list := conf.get("metrics", "metrics_allow_list", fallback=None)),
        "block": (metric_block_list := conf.get("metrics", "metrics_block_list", fallback=None)),
    }

    if metric_allow_list:
        list_type = "allow"
        if metric_block_list:
            log.warning(
                "Ignoring metrics_block_list as both metrics_allow_list and metrics_block_list have been set."
            )
    elif metric_block_list:
        list_type = "block"
    else:
        list_type = DEFAULT_VALIDATOR_TYPE

    return validators[list_type](metric_lists[list_type])


def validate_stat(fn: Callable) -> Callable:
    """Check if stat name contains invalid characters; logs and does not emit stats if name is invalid."""

    @wraps(fn)
    def wrapper(self, stat: str | None = None, *args, **kwargs) -> Callable | None:
        try:
            if stat is not None:
                handler_stat_name_func = get_current_handler_stat_name_func()
                stat = handler_stat_name_func(stat)
            return fn(self, stat, *args, **kwargs)
        except InvalidStatsNameException:
            log.exception("Invalid stat name: %s.", stat)
            return None

    return cast("Callable", wrapper)


def stat_name_otel_handler(
    stat_prefix: str,
    stat_name: str,
    max_length: int = OTEL_NAME_MAX_LENGTH,
) -> str:
    """
    Verify that a proposed prefix and name combination will meet OpenTelemetry naming standards.

    See: https://opentelemetry.io/docs/reference/specification/metrics/api/#instrument-name-syntax

    :param stat_prefix: The proposed prefix applied to all metric names.
    :param stat_name: The proposed name.
    :param max_length: The max length of the combined prefix and name; defaults to the max length
        as defined in the OpenTelemetry standard, but can be overridden.

    :returns: Returns the approved combined name or raises an InvalidStatsNameException.
    """
    proposed_stat_name: str = f"{stat_prefix}.{stat_name}"
    name_length_exemption: bool = False
    matched_exemption: str = ""

    # This test case is here to enforce that the values can not be None and
    # must be a valid String.  Without this test here, those values get cast
    # to a string and pass when they should not, potentially resulting in
    # metrics named "airflow.None", "airflow.42", or "None.42" for example.
    if not (isinstance(stat_name, str) and isinstance(stat_prefix, str)):
        raise InvalidStatsNameException("Stat name and prefix must both be strings.")

    if len(proposed_stat_name) > OTEL_NAME_MAX_LENGTH:
        # If the name is in the exceptions list, do not fail it for being too long.
        # It may still be deemed invalid for other reasons below.
        for exemption in BACK_COMPAT_METRIC_NAMES:
            if re.match(exemption, stat_name):
                # There is a back-compat exception for this name; proceed
                name_length_exemption = True
                matched_exemption = exemption.pattern
                break
        else:
            raise InvalidStatsNameException(
                f"Invalid stat name: {proposed_stat_name}.  Please see "
                f"https://opentelemetry.io/docs/reference/specification/metrics/api/#instrument-name-syntax"
            )

    # `stat_name_default_handler` throws InvalidStatsNameException if the
    # provided value is not valid or returns the value if it is.  We don't
    # need the return value but will make use of the validation checks. If
    # no exception is thrown, then the proposed name meets OTel requirements.
    stat_name_default_handler(proposed_stat_name, max_length=999 if name_length_exemption else max_length)

    # This warning is down here instead of up above because the exemption only
    # applies to the length and a name may still be invalid for other reasons.
    if name_length_exemption:
        warnings.warn(
            f"Stat name {stat_name} matches exemption {matched_exemption} and "
            f"will be truncated to {proposed_stat_name[:OTEL_NAME_MAX_LENGTH]}. "
            f"This stat name will be deprecated in the future and replaced with "
            f"a shorter name combined with Attributes/Tags.",
            MetricNameLengthExemptionWarning,
            stacklevel=2,
        )

    return proposed_stat_name


def stat_name_default_handler(
    stat_name: str, max_length: int = 250, allowed_chars: Iterable[str] = ALLOWED_CHARACTERS
) -> str:
    """
    Validate the metric stat name.

    Apply changes when necessary and return the transformed stat name.
    """
    if not isinstance(stat_name, str):
        raise InvalidStatsNameException("The stat_name has to be a string")
    if len(stat_name) > max_length:
        raise InvalidStatsNameException(
            f"The stat_name ({stat_name}) has to be less than {max_length} characters."
        )
    if any(c not in allowed_chars for c in stat_name):
        raise InvalidStatsNameException(
            f"The stat name ({stat_name}) has to be composed of ASCII "
            f"alphabets, numbers, or the underscore, dot, or dash characters."
        )
    return stat_name


def get_current_handler_stat_name_func() -> Callable[[str], str]:
    """Get Stat Name Handler from airflow.cfg."""
    handler = conf.getimport("metrics", "stat_name_handler")
    if handler is None:
        if conf.get("metrics", "statsd_influxdb_enabled", fallback=False):
            handler = partial(stat_name_default_handler, allowed_chars={*ALLOWED_CHARACTERS, ",", "="})
        else:
            handler = stat_name_default_handler
    return handler


class ListValidator(metaclass=abc.ABCMeta):
    """
    ListValidator metaclass that can be implemented as a AllowListValidator or BlockListValidator.

    The test method must be overridden by its subclass.
    """

    def __init__(self, validate_list: str | None = None) -> None:
        self.validate_list: tuple[str, ...] | None = (
            tuple(item.strip().lower() for item in validate_list.split(",")) if validate_list else None
        )

    @classmethod
    def __subclasshook__(cls, subclass: Callable[[str], str]) -> bool:
        return hasattr(subclass, "test") and callable(subclass.test) or NotImplemented

    @abc.abstractmethod
    def test(self, name: str) -> bool:
        """Test if name is allowed."""
        raise NotImplementedError

    def _has_pattern_match(self, name: str) -> bool:
        for entry in self.validate_list or ():
            if re.findall(entry, name.strip().lower()):
                return True
        return False


class PatternAllowListValidator(ListValidator):
    """Match the provided strings anywhere in the metric name."""

    def test(self, name: str) -> bool:
        if self.validate_list is not None:
            return super()._has_pattern_match(name)
        return True  # default is all metrics are allowed


class PatternBlockListValidator(ListValidator):
    """Only allow names that do not match the blocked strings."""

    def test(self, name: str) -> bool:
        if self.validate_list is not None:
            return not super()._has_pattern_match(name)
        return True  # default is all metrics are allowed
