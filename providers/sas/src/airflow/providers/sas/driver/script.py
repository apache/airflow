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
"""The SAS programs the hook writes: settings, ``PROC SQL`` scripts, SAS literals for bind parameters and paging clauses."""

from __future__ import annotations

import datetime
import math
import numbers
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from decimal import Decimal
from functools import singledispatch
from typing import Any

# Every ``<name>.<name>`` in the SQL: the names that ``sas_libraries`` knows
# are libraries, the others (column aliases ``a.id``, ``DICTIONARY``) are
# ignored.  Matching only after FROM / JOIN missed ``FROM lib1.a, lib2.b``.
_SCHEMA_RE = re.compile(r"\b(\w+)\.\w+")

# Whitespace, comments and opening parentheses that may precede the first
# keyword of a query: ``/* extract */ (SELECT ...``.
_LEADING_NOISE_RE = re.compile(r"(?:\s+|--[^\n]*|/\*.*?\*/|\()*", re.DOTALL)

_MONTHS = (
    "JAN", "FEB", "MAR", "APR", "MAY", "JUN",
    "JUL", "AUG", "SEP", "OCT", "NOV", "DEC",
)  # fmt: skip

# Member name of a query result in the cursor's output library.
_RESULT_TABLE = "result"

_SAS_COMPRESS_VALUES: frozenset[str] = frozenset({"yes", "no", "char", "binary"})

# The paging clause ``GenericTransfer`` appends to a query (its default
# ``paginated_sql_statement_clause``): SAS has no LIMIT / OFFSET, the hook
# serves the pages itself (:class:`~airflow.providers.sas.driver.job.SASPagedRead`).
_PAGE_RE = re.compile(r"\s+LIMIT\s+(\d+)\s+OFFSET\s+(\d+)\s*;?\s*$", re.IGNORECASE)


@dataclass(frozen=True, kw_only=True)
class SASSettings:
    """
    Everything a SAS run needs besides the SSH session.

    ``SASHook`` resolves it once per connection (constructor argument, then
    connection extra, then the defaults below) and every connection and
    cursor reads it from here.  The values are normalised on creation:
    library names lower-cased for the lookup in the SQL, options as a
    tuple, ``timeout`` as a float and ``sas_compress`` validated.

    :param sas_binary: Path to the SAS binary on the remote server.
    :param sas_options: Command-line options passed to SAS.
    :param sas_libraries: SAS library name to its directory on the server.
    :param sas_temp_dir: Remote directory for the script, log and result.
    :param sas_work_dir: Root of the SAS ``WORK`` library; ``sas_temp_dir``
        when ``None``.
    :param encoding: Encoding of the ``.sas7bdat`` result and the SAS log.
    :param limit: Default server-side row limit (``PROC SQL OUTOBS=``).
    :param timeout: Seconds to wait for one SAS run; ``None`` waits forever.
    :param sas_compress: ``COMPRESS=`` dataset option of query results.
    """

    sas_binary: str = "/opt/sas/SASFoundation/9.4/sas"
    sas_options: tuple[str, ...] = (
        "-linesize 90",
        "-noterminal",
        "-nodate",
        "-nonews",
    )
    sas_libraries: Mapping[str, str] = field(default_factory=dict)
    sas_temp_dir: str = "/tmp"
    sas_work_dir: str | None = None
    encoding: str = "iso-8859-15"
    limit: int | None = None
    timeout: float | None = None
    sas_compress: str = "yes"

    def __post_init__(self) -> None:
        compress = str(self.sas_compress).strip().lower()
        if compress not in _SAS_COMPRESS_VALUES:
            raise ValueError(
                f"Unsupported sas_compress value {self.sas_compress!r}; "
                f"expected one of {sorted(_SAS_COMPRESS_VALUES)}"
            )
        # A frozen dataclass normalises its own fields through object.__setattr__.
        object.__setattr__(self, "sas_compress", compress)
        object.__setattr__(self, "sas_options", tuple(self.sas_options))
        object.__setattr__(
            self,
            "sas_libraries",
            {name.lower(): path for name, path in self.sas_libraries.items()},
        )
        if self.timeout is not None:
            object.__setattr__(self, "timeout", float(self.timeout))


class SASScriptError(Exception):
    """Raised when a remote SAS script execution fails (return code >= 2)."""


@singledispatch
def sas_literal(value: Any) -> str:
    """
    Render *value* as a ``PROC SQL`` literal for a ``%s`` bind parameter.

    SAS has no client-side parameter binding: the values are written into
    the script, so each one must become a literal SAS parses back to the
    same value.  Strings are single-quoted (double quotes would let SAS
    resolve ``&macro`` references inside them), dates and datetimes use
    the ``'20OCT2026'd`` / ``'20OCT2026:10:00:00'dt`` forms with English
    month names whatever the locale.  A type without a literal raises
    instead of being written with ``str()``.
    """
    raise TypeError(f"Cannot bind {type(value).__name__} value {value!r} as a SAS literal")


@sas_literal.register(type(None))
def _(value: None) -> str:
    # SAS has no NULL literal: ``.`` is the numeric missing value and ``''``
    # the character one, and the column type is not known here.
    raise TypeError(
        "Cannot bind None: SAS has no NULL literal, write 'IS MISSING' in the SQL or bind '' or float('nan')"
    )


@sas_literal.register(str)
def _(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


@sas_literal.register(bool)
def _(value: bool) -> str:
    return "1" if value else "0"


@sas_literal.register(numbers.Integral)
def _(value: numbers.Integral) -> str:
    return str(int(value))


@sas_literal.register(numbers.Real)
def _(value: numbers.Real) -> str:
    number = float(value)
    if math.isnan(number):
        return "."
    if math.isinf(number):
        raise ValueError(f"Cannot bind {value!r}: SAS has no infinite numeric")
    return repr(number)


@sas_literal.register(Decimal)
def _(value: Decimal) -> str:
    if value.is_nan():
        return "."
    if not value.is_finite():
        raise ValueError(f"Cannot bind {value!r}: SAS has no infinite numeric")
    return str(value)


@sas_literal.register(datetime.date)
def _(value: datetime.date) -> str:
    return f"'{value.day:02d}{_MONTHS[value.month - 1]}{value.year:04d}'d"


@sas_literal.register(datetime.datetime)
def _(value: datetime.datetime) -> str:
    # The wall-clock time as given: SAS datetimes carry no time zone.
    fraction = f".{value.microsecond:06d}" if value.microsecond else ""
    return f"'{value.day:02d}{_MONTHS[value.month - 1]}{value.year:04d}:{value:%H:%M:%S}{fraction}'dt"


class SASScript:
    """
    Write the ``PROC SQL`` scripts a cursor runs.

    Every library the SQL references that ``sas_libraries`` knows gets a
    ``LIBNAME``; a query stores its result as ``out.result``, with the
    ``COMPRESS=`` option of the connection.  The boilerplate is cached per
    set of libraries (and ``OUTOBS`` for queries), with the output library
    path left as a placeholder, so one instance serves every cursor of a
    connection.
    """

    def __init__(self, sas_libraries: Mapping[str, str], compress: str = "no") -> None:
        self._sas_libraries = sas_libraries
        self._compress = None if compress == "no" else compress
        self._query_templates: dict[tuple[tuple[tuple[str, str], ...], int | None], str] = {}
        self._statement_templates: dict[tuple[tuple[str, str], ...], str] = {}

    def libnames(self, sql: str) -> tuple[tuple[str, str], ...]:
        """
        Return the ``(name, path)`` of every library *sql* references.

        Every ``<name>.<name>`` in the SQL is a candidate, in order of first
        appearance; the names ``sas_libraries`` does not know (column aliases
        such as ``a.id``, ``DICTIONARY``, ``WORK``, a library the SAS session
        assigns itself) are left to SAS.
        """
        candidates = dict.fromkeys(match.lower() for match in _SCHEMA_RE.findall(sql))
        return tuple((name, self._sas_libraries[name]) for name in candidates if name in self._sas_libraries)

    @staticmethod
    def _libname_statements(libnames: tuple[tuple[str, str], ...]) -> str:
        """One ``LIBNAME`` statement per library, the path as a SAS string."""
        return "".join(f"libname {name} {sas_literal(path)};\n" for name, path in libnames)

    @staticmethod
    def _terminated(sql: str) -> str:
        return sql if sql.rstrip().endswith(";") else f"{sql};"

    def query(self, sql: str, out_dir: str, outobs: int | None = None) -> str:
        """
        Build a script that runs a SELECT and stores its result in *out_dir*.

        :param out_dir: Remote directory assigned as the ``out`` library.
        :param outobs: When set, limits the result server-side with
            ``PROC SQL OUTOBS=``; ``None`` or ``0`` means no limit.
        """
        libnames = self.libnames(sql)
        outobs = outobs or None
        template = self._query_templates.get((libnames, outobs))
        if template is None:
            _outobs = f" outobs={outobs}" if outobs else ""
            _compress = f" (compress={self._compress})" if self._compress else ""
            template = (
                f"{self._libname_statements(libnames)}libname out {{1}};\n\n"
                f"proc sql{_outobs};\n"
                f"create table out.{_RESULT_TABLE}{_compress} as\n{{0}}\nquit;\n"
            )
            self._query_templates[(libnames, outobs)] = template
        return template.format(self._terminated(sql), sas_literal(out_dir))

    @staticmethod
    def bind(sql: str, parameters: list | tuple | Mapping[str, Any] | None) -> str:
        """
        Write ``%s`` / ``%(name)s`` bind parameters into *sql* as SAS literals.

        Each value goes through :func:`sas_literal`, so a string arrives
        quoted and escaped, never as SQL text.  As with every ``pyformat``
        driver, a literal ``%`` in *sql* must be written ``%%`` when
        parameters are given.  The result is stripped.
        """
        if not parameters:
            return sql.strip()
        if isinstance(parameters, Mapping):
            bound = sql % {name: sas_literal(value) for name, value in parameters.items()}
        else:
            bound = sql % tuple(map(sas_literal, parameters))
        return bound.strip()

    @staticmethod
    def page(sql: str) -> SASPage | None:
        """
        Split a trailing ``LIMIT n OFFSET m`` off *sql*, or return ``None``.

        The clause is the one ``GenericTransfer`` appends by default; SAS
        has no LIMIT / OFFSET and :class:`~airflow.providers.sas.driver.job.SASPagedRead` serves the pages.
        A ``LIMIT`` without ``OFFSET`` is not translated: the hook's
        ``limit`` (``OUTOBS=``) is the SAS way to cap a query.
        """
        match = _PAGE_RE.search(sql)
        if match is None:
            return None
        return SASPage(sql[: match.start()], int(match[1]), int(match[2]))

    @staticmethod
    def is_query(sql: str) -> bool:
        """
        Return ``True`` when *sql* is expected to produce a result set.

        Leading comments and opening parentheses are skipped, so
        ``/* extract */ SELECT ...`` is a query and not a statement whose
        empty result would go unnoticed.
        """
        # The pattern also matches the empty string, so there always is a match.
        start = match.end() if (match := _LEADING_NOISE_RE.match(sql)) else 0
        return sql[start:].upper().startswith(("SELECT", "WITH"))

    def bound_query(
        self,
        sql: str,
        parameters: list | tuple | Mapping[str, Any] | None,
        out_dir: str,
        outobs: int | None = None,
    ) -> str:
        """
        Bind *parameters*, check that *sql* is a query and write its script.

        :raises ValueError: If *sql* is not a data-returning query.
        """
        bound = self.bind(sql, parameters)
        if not self.is_query(bound):
            raise ValueError("Only SELECT / WITH queries return a DataFrame")
        return self.query(bound, out_dir, outobs=outobs)

    def statement(self, sql: str) -> str:
        """Build a script that runs a non-SELECT SQL statement."""
        libnames = self.libnames(sql)
        template = self._statement_templates.get(libnames)
        if template is None:
            separator = "\n" if libnames else ""
            template = f"{self._libname_statements(libnames)}{separator}proc sql;\n{{0}}\nquit;\n"
            self._statement_templates[libnames] = template
        return template.format(self._terminated(sql))


@dataclass(frozen=True)
class SASPage:
    """One ``LIMIT size OFFSET offset`` page of *sql*, the query without the clause."""

    sql: str
    size: int
    offset: int

    @property
    def end(self) -> int:
        """Offset of the page that follows this one."""
        return self.offset + self.size
