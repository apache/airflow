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

import re
from abc import abstractmethod
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

from airflow._shared.timezones.timezone import make_aware, parse_timezone
from airflow.partition_mappers.base import PartitionMapper

if TYPE_CHECKING:
    from pendulum import FixedTimezone, Timezone


_STRPTIME_PATTERNS: dict[str, str] = {
    "%Y": r"\d{4}",
    "%m": r"\d{2}",
    "%d": r"\d{2}",
    "%H": r"\d{2}",
    "%M": r"\d{2}",
    "%S": r"\d{2}",
    "%V": r"\d{2}",
    "%U": r"\d{2}",
    "%W": r"\d{2}",
}


def _compile_output_format_regex(
    fmt: str, placeholder_patterns: dict[str, str] | None = None
) -> re.Pattern[str]:
    r"""
    Compile *fmt* into a regex with named groups so a formatted key can be parsed back.

    Walks *fmt* left-to-right, classifying each position into one of three
    branches:

    1. A two-character strftime directive listed in ``_STRPTIME_PATTERNS``
       (``%Y``, ``%m``, ``%V`` …) becomes a named group keyed on the directive
       letter — e.g. ``%Y`` → ``(?P<Y>\d{4})``. The ``i + 1 < len(fmt)`` guard
       lets a trailing bare ``%`` fall through to the literal branch (escaped
       as ``\%``), and unrecognised directives like ``%X`` likewise fall
       through (matched literally; the format will not round-trip through
       strftime output that uses them).
    2. A FastAPI-style ``{name}`` placeholder becomes a named group keyed on
       *name*. The pattern defaults to ``\w+``; pass *placeholder_patterns*
       to narrow it (e.g. ``{"quarter": r"[1-4]"}``). A ``{`` with no
       matching ``}`` falls through to the literal branch (so a stray ``{``
       is escaped, not raised). This ``{name}`` → named-group translation
       mirrors the path-template parsing in Starlette's ``compile_path``
       (see `Starlette routing <https://www.starlette.io/routing/>`_).
    3. Anything else is escaped via :func:`re.escape` and emitted verbatim,
       so separators, literal hyphens, ``Q``/``W`` prefix letters, and so on
       all participate in the match.

    The compiled regex is anchored with ``^...$`` so callers can use
    ``.search`` or ``.fullmatch`` interchangeably — a malformed key with
    extra prefix/suffix raises instead of silently parsing the first
    matching substring.

    Raises :exc:`ValueError` at compile time when *fmt* is structurally
    malformed — empty ``{}``, a placeholder name that is not a valid Python
    identifier, the same strftime directive used twice, the same
    placeholder used twice, or two adjacent default-pattern placeholders
    (the greedy ``\w+`` default would consume the previous group's tail,
    yielding a parse the caller almost never intends — insert a separator
    or narrow at least one placeholder via *placeholder_patterns*).
    Catching these at construction keeps a misconfigured mapper from
    surfacing as an opaque ``re.error`` deep inside the scheduler tick or
    a UI route.

    Known limitation: locale-sensitive strftime directives (``%A``, ``%a``,
    ``%B``, ``%b``, ``%p``) are not in ``_STRPTIME_PATTERNS`` and are
    treated as literals; formats relying on them are not invertible by
    this helper.
    """
    placeholder_patterns = placeholder_patterns or {}
    parts: list[str] = []
    seen_groups: set[str] = set()
    prev_was_default_placeholder = False
    i = 0
    while i < len(fmt):
        if fmt[i] == "%" and i + 1 < len(fmt) and fmt[i : i + 2] in _STRPTIME_PATTERNS:
            directive = fmt[i : i + 2]
            name = directive[1]
            if name in seen_groups:
                raise ValueError(
                    f"output_format {fmt!r} reuses directive {directive!r}; "
                    "each strftime directive may appear at most once."
                )
            seen_groups.add(name)
            parts.append(f"(?P<{name}>{_STRPTIME_PATTERNS[directive]})")
            i += 2
            prev_was_default_placeholder = False
            continue
        if fmt[i] == "{":
            end = fmt.find("}", i)
            if end != -1:
                name = fmt[i + 1 : end]
                if not name.isidentifier():
                    raise ValueError(
                        f"output_format {fmt!r} has invalid placeholder {{{name}}}; "
                        "placeholder names must be valid Python identifiers."
                    )
                if name in seen_groups:
                    raise ValueError(
                        f"output_format {fmt!r} reuses placeholder {{{name}}}; "
                        "each placeholder may appear at most once."
                    )
                seen_groups.add(name)
                is_default = name not in placeholder_patterns
                if is_default and prev_was_default_placeholder:
                    # Two adjacent default-pattern (``\w+``) placeholders.
                    # The greedy first group consumes everything except the
                    # minimum length the second needs, producing a parse the
                    # caller almost never intends (e.g. ``{a}{b}`` on "foo123"
                    # yields a="foo12", b="3"). Either insert a literal
                    # separator between them or narrow at least one pattern
                    # via *placeholder_patterns*.
                    raise ValueError(
                        f"output_format {fmt!r} has adjacent default-pattern placeholders "
                        f"ending at {{{name}}}; the greedy ``\\w+`` default would consume "
                        "the previous group's tail. Insert a separator between the "
                        "placeholders, or narrow at least one of them via placeholder_patterns."
                    )
                pattern = placeholder_patterns.get(name, r"\w+")
                parts.append(f"(?P<{name}>{pattern})")
                i = end + 1
                prev_was_default_placeholder = is_default
                continue
        parts.append(re.escape(fmt[i]))
        i += 1
        prev_was_default_placeholder = False
    return re.compile(f"^{''.join(parts)}$")


class _BaseTemporalMapper(PartitionMapper):
    """Base class for Temporal Partition Mappers."""

    default_output_format: str

    def __init__(
        self,
        *,
        timezone: str | Timezone | FixedTimezone = "UTC",
        input_format: str = "%Y-%m-%dT%H:%M:%S",
        output_format: str | None = None,
    ):
        self.input_format = input_format
        self.output_format = output_format or self.default_output_format
        if isinstance(timezone, str):
            timezone = parse_timezone(timezone)
        self._timezone = timezone

    def to_downstream(self, key: str) -> str:
        dt = datetime.strptime(key, self.input_format)
        if dt.tzinfo is None:
            dt = make_aware(dt, self._timezone)
        else:
            dt = dt.astimezone(self._timezone)
        normalized = self.normalize(dt)
        return self.format(normalized)

    @abstractmethod
    def normalize(self, dt: datetime) -> datetime:
        """Return canonical start datetime for the partition."""

    def format(self, dt: datetime) -> str:
        """Format the normalized datetime."""
        return dt.strftime(self.output_format)

    def decode_downstream(self, downstream_key: str) -> datetime:
        """
        Recover the period-start datetime from a previously formatted downstream key.

        Inverse of ``format``. The default implementation uses ``strptime`` with
        ``output_format``, which works for any format made of standard strptime
        directives. Subclasses with custom format markers (e.g. ``{quarter}``) or
        ambiguous directives (e.g. bare ``%V``) override this.
        """
        return datetime.strptime(downstream_key, self.output_format)

    def encode_upstream(self, dt: datetime) -> str:
        """
        Format *dt* as an upstream partition key string.

        Pair of :meth:`decode_downstream`: takes a (decoded) period-start
        datetime and produces a key string in the upstream's ``input_format``
        with ``timezone`` applied. Used by :class:`RollupMapper` to render each
        upstream member yielded by the window back into the form upstream
        producers actually emit.

        When *dt* is aware (e.g. the default ``decode_downstream`` returned an
        aware datetime because ``output_format`` contains ``%z``), converts to
        the mapper timezone first so the resulting key reflects the same instant
        rather than re-interpreting the wall-clock as local time.
        """
        if dt.tzinfo is not None:
            naive = dt.astimezone(self._timezone).replace(tzinfo=None)
        else:
            naive = dt
        return make_aware(naive, self._timezone).strftime(self.input_format)

    def serialize(self) -> dict[str, Any]:
        from airflow.serialization.encoders import encode_timezone

        return {
            "timezone": encode_timezone(self._timezone),
            "input_format": self.input_format,
            "output_format": self.output_format,
        }

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> PartitionMapper:
        return cls(
            timezone=parse_timezone(data.get("timezone", "UTC")),
            input_format=data["input_format"],
            output_format=data["output_format"],
        )


class StartOfHourMapper(_BaseTemporalMapper):
    """
    Map a partition key to the start of the hour that contains the key.

    Example: ``2024-03-13T10:42:15`` → ``2024-03-13T10``.
    """

    default_output_format = "%Y-%m-%dT%H"

    def normalize(self, dt: datetime) -> datetime:
        return dt.replace(minute=0, second=0, microsecond=0)


class StartOfDayMapper(_BaseTemporalMapper):
    """
    Map a partition key to the start of the day that contains the key.

    Example: ``2024-03-13T10:42:15`` → ``2024-03-13``.
    """

    default_output_format = "%Y-%m-%d"

    def normalize(self, dt: datetime) -> datetime:
        return dt.replace(hour=0, minute=0, second=0, microsecond=0)


class StartOfWeekMapper(_BaseTemporalMapper):
    """
    Map a partition key to the Monday of the ISO week that contains the key.

    Example: ``2024-03-13T10:42:15`` → ``2024-03-11 (W11)``.
    """

    default_output_format = "%Y-%m-%d (W%V)"

    def __init__(
        self,
        *,
        timezone: str | Timezone | FixedTimezone = "UTC",
        input_format: str = "%Y-%m-%dT%H:%M:%S",
        output_format: str | None = None,
    ) -> None:
        """
        Compile *output_format* eagerly so malformed patterns raise here.

        :param timezone: Timezone used to localize naive upstream keys before
            normalising to week-start. Accepts a timezone name (e.g.
            ``"America/New_York"``), a ``pendulum.Timezone``, or a
            ``pendulum.FixedTimezone``.
        :param input_format: ``strptime``-compatible format for parsing upstream
            partition keys. Supported directives (those that appear in
            ``_STRPTIME_PATTERNS``): ``%Y %m %d %H %M %S %V %U %W``.
        :param output_format: ``strftime``-compatible format for the downstream
            (week-start) key. To support ``decode_downstream``, the format
            **must** include ``%Y``, ``%m``, and ``%d`` so the week-start date
            can be recovered for ``to_upstream``.
        """
        super().__init__(timezone=timezone, input_format=input_format, output_format=output_format)
        # %V (ISO week) cannot be round-tripped through strptime without %G+%u,
        # so derive a named-group regex from output_format and pull out %Y/%m/%d.
        # Compile eagerly so a malformed output_format raises ValueError here
        # instead of an opaque re.error inside the scheduler.
        self._key_pattern = _compile_output_format_regex(self.output_format)

    def normalize(self, dt: datetime) -> datetime:
        start = dt - timedelta(days=dt.weekday())
        return start.replace(hour=0, minute=0, second=0, microsecond=0)

    def decode_downstream(self, downstream_key: str) -> datetime:
        match = self._key_pattern.search(downstream_key)
        if match is None or not {"Y", "m", "d"}.issubset(match.groupdict()):
            raise ValueError(
                f"StartOfWeekMapper.decode_downstream could not parse {downstream_key!r} "
                f"with output_format {self.output_format!r}; "
                "output_format must include the %Y, %m and %d directives."
            )
        return datetime(int(match["Y"]), int(match["m"]), int(match["d"]))


class StartOfMonthMapper(_BaseTemporalMapper):
    """
    Map a partition key to day 1 of the month that contains the key.

    Example: ``2024-03-13T10:42:15`` → ``2024-03``.
    """

    default_output_format = "%Y-%m"

    def normalize(self, dt: datetime) -> datetime:
        return dt.replace(
            day=1,
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )


class StartOfQuarterMapper(_BaseTemporalMapper):
    """
    Map a partition key to the first day of the calendar quarter that contains the key.

    Example: ``2024-03-13T10:42:15`` → ``2024-Q1``.
    """

    default_output_format = "%Y-Q{quarter}"

    def __init__(
        self,
        *,
        timezone: str | Timezone | FixedTimezone = "UTC",
        input_format: str = "%Y-%m-%dT%H:%M:%S",
        output_format: str | None = None,
    ) -> None:
        """
        Compile *output_format* eagerly so malformed patterns raise here.

        :param timezone: Timezone used to localize naive upstream keys before
            normalising to quarter-start. Accepts a timezone name (e.g.
            ``"America/New_York"``), a ``pendulum.Timezone``, or a
            ``pendulum.FixedTimezone``.
        :param input_format: ``strptime``-compatible format for parsing upstream
            partition keys. Supported directives (those that appear in
            ``_STRPTIME_PATTERNS``): ``%Y %m %d %H %M %S %V %U %W``.
        :param output_format: Format for the downstream (quarter-start) key.
            Supports the same ``strftime`` directives as *input_format* plus the
            Python-format ``{quarter}`` placeholder (e.g. ``"%Y-Q{quarter}"``).
            To support ``decode_downstream``, the format **must** include ``%Y``
            and ``{quarter}`` so the quarter-start date can be recovered for
            ``to_upstream``.
        """
        super().__init__(timezone=timezone, input_format=input_format, output_format=output_format)
        # ``{quarter}`` is a Python-format placeholder, not a strftime directive,
        # so derive a named-group regex from output_format that handles both.
        # Compile eagerly so a malformed output_format raises ValueError here
        # instead of an opaque re.error inside the scheduler.
        self._key_pattern = _compile_output_format_regex(self.output_format, {"quarter": r"[1-4]"})

    def normalize(self, dt: datetime) -> datetime:
        quarter = (dt.month - 1) // 3
        month = quarter * 3 + 1
        return dt.replace(
            month=month,
            day=1,
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )

    def format(self, dt: datetime) -> str:
        quarter = (dt.month - 1) // 3 + 1
        return dt.strftime(self.output_format).format(quarter=quarter)

    def decode_downstream(self, downstream_key: str) -> datetime:
        match = self._key_pattern.search(downstream_key)
        if match is None or not {"Y", "quarter"}.issubset(match.groupdict()):
            raise ValueError(
                f"StartOfQuarterMapper.decode_downstream could not parse {downstream_key!r} "
                f"with output_format {self.output_format!r}; "
                "output_format must include the %Y directive and the {quarter} placeholder."
            )
        year = int(match["Y"])
        quarter = int(match["quarter"])
        return datetime(year, (quarter - 1) * 3 + 1, 1)


class StartOfYearMapper(_BaseTemporalMapper):
    """
    Map a partition key to January 1 of the year that contains the key.

    Example: ``2024-03-13T10:42:15`` → ``2024``.
    """

    default_output_format = "%Y"

    def normalize(self, dt: datetime) -> datetime:
        return dt.replace(
            month=1,
            day=1,
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )
