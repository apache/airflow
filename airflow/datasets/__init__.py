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

import os
import urllib.parse
import warnings
from typing import TYPE_CHECKING, Any, Callable, ClassVar, Iterable, Iterator

import attr

if TYPE_CHECKING:
    from urllib.parse import SplitResult

__all__ = ["Dataset", "DatasetAll", "DatasetAny"]


def normalize_noop(parts: SplitResult) -> SplitResult:
    return parts


def _get_uri_normalizer(scheme: str) -> Callable[[SplitResult], SplitResult] | None:
    if scheme == "file":
        return normalize_noop
    from airflow.providers_manager import ProvidersManager

    return ProvidersManager().dataset_uri_handlers.get(scheme)


def _sanitize_uri(uri: str) -> str:
    if not uri:
        raise ValueError("Dataset URI cannot be empty")
    if uri.isspace():
        raise ValueError("Dataset URI cannot be just whitespace")
    if not uri.isascii():
        raise ValueError("Dataset URI must only consist of ASCII characters")
    parsed = urllib.parse.urlsplit(uri)
    if not parsed.scheme and not parsed.netloc:  # Does not look like a URI.
        return uri
    normalized_scheme = parsed.scheme.lower()
    if normalized_scheme.startswith("x-"):
        return uri
    if normalized_scheme == "airflow":
        raise ValueError("Dataset scheme 'airflow' is reserved")
    _, auth_exists, normalized_netloc = parsed.netloc.rpartition("@")
    if auth_exists:
        # TODO: Collect this into a DagWarning.
        warnings.warn(
            "A dataset URI should not contain auth info (e.g. username or "
            "password). It has been automatically dropped.",
            UserWarning,
            stacklevel=3,
        )
    if parsed.query:
        normalized_query = urllib.parse.urlencode(sorted(urllib.parse.parse_qsl(parsed.query)))
    else:
        normalized_query = ""
    parsed = parsed._replace(
        scheme=normalized_scheme,
        netloc=normalized_netloc,
        path=parsed.path.rstrip("/") or "/",  # Remove all trailing slashes.
        query=normalized_query,
        fragment="",  # Ignore any fragments.
    )
    if (normalizer := _get_uri_normalizer(normalized_scheme)) is not None:
        parsed = normalizer(parsed)
    return urllib.parse.urlunsplit(parsed)


class BaseDatasetEventInput:
    """Protocol for all dataset triggers to use in ``DAG(schedule=...)``.

    :meta private:
    """

    def __or__(self, other: BaseDatasetEventInput) -> DatasetAny:
        if not isinstance(other, BaseDatasetEventInput):
            return NotImplemented
        return DatasetAny(self, other)

    def __and__(self, other: BaseDatasetEventInput) -> DatasetAll:
        if not isinstance(other, BaseDatasetEventInput):
            return NotImplemented
        return DatasetAll(self, other)

    def evaluate(self, statuses: dict[str, bool]) -> bool:
        raise NotImplementedError

    def iter_datasets(self) -> Iterator[tuple[str, Dataset]]:
        raise NotImplementedError


@attr.define()
class Dataset(os.PathLike, BaseDatasetEventInput):
    """A representation of data dependencies between workflows."""

    uri: str = attr.field(
        converter=_sanitize_uri,
        validator=[attr.validators.min_len(1), attr.validators.max_len(3000)],
    )
    extra: dict[str, Any] | None = None

    __version__: ClassVar[int] = 1

    def __fspath__(self) -> str:
        return self.uri

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, self.__class__):
            return self.uri == other.uri
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self.uri)

    def iter_datasets(self) -> Iterator[tuple[str, Dataset]]:
        yield self.uri, self

    def evaluate(self, statuses: dict[str, bool]) -> bool:
        return statuses.get(self.uri, False)


class _DatasetBooleanCondition(BaseDatasetEventInput):
    """Base class for dataset boolean logic."""

    agg_func: Callable[[Iterable], bool]

    def __init__(self, *objects: BaseDatasetEventInput) -> None:
        if not all(isinstance(o, BaseDatasetEventInput) for o in objects):
            raise TypeError("expect dataset expressions in condition")
        self.objects = objects

    def evaluate(self, statuses: dict[str, bool]) -> bool:
        return self.agg_func(x.evaluate(statuses=statuses) for x in self.objects)

    def iter_datasets(self) -> Iterator[tuple[str, Dataset]]:
        seen = set()  # We want to keep the first instance.
        for o in self.objects:
            for k, v in o.iter_datasets():
                if k in seen:
                    continue
                yield k, v
                seen.add(k)


class DatasetAny(_DatasetBooleanCondition):
    """Use to combine datasets schedule references in an "and" relationship."""

    agg_func = any

    def __or__(self, other: BaseDatasetEventInput) -> DatasetAny:
        if not isinstance(other, BaseDatasetEventInput):
            return NotImplemented
        # Optimization: X | (Y | Z) is equivalent to X | Y | Z.
        return DatasetAny(*self.objects, other)

    def __repr__(self) -> str:
        return f"DatasetAny({', '.join(map(str, self.objects))})"


class DatasetAll(_DatasetBooleanCondition):
    """Use to combine datasets schedule references in an "or" relationship."""

    agg_func = all

    def __and__(self, other: BaseDatasetEventInput) -> DatasetAll:
        if not isinstance(other, BaseDatasetEventInput):
            return NotImplemented
        # Optimization: X & (Y & Z) is equivalent to X & Y & Z.
        return DatasetAll(*self.objects, other)

    def __repr__(self) -> str:
        return f"DatasetAll({', '.join(map(str, self.objects))})"
