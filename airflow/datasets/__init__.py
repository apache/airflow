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
from typing import Any, Callable, ClassVar, Iterable, Iterator, Protocol, runtime_checkable
from urllib.parse import urlsplit

import attr

__all__ = ["Dataset", "DatasetAll", "DatasetAny"]


@runtime_checkable
class BaseDatasetEventInput(Protocol):
    """Protocol for all dataset triggers to use in ``DAG(schedule=...)``.

    :meta private:
    """

    def evaluate(self, statuses: dict[str, bool]) -> bool:
        raise NotImplementedError

    def iter_datasets(self) -> Iterator[tuple[str, Dataset]]:
        raise NotImplementedError


@attr.define()
class Dataset(os.PathLike, BaseDatasetEventInput):
    """A representation of data dependencies between workflows."""

    uri: str = attr.field(validator=[attr.validators.min_len(1), attr.validators.max_len(3000)])
    extra: dict[str, Any] | None = None

    __version__: ClassVar[int] = 1

    @uri.validator
    def _check_uri(self, attr, uri: str):
        if uri.isspace():
            raise ValueError(f"{attr.name} cannot be just whitespace")
        try:
            uri.encode("ascii")
        except UnicodeEncodeError:
            raise ValueError(f"{attr.name!r} must be ascii")
        parsed = urlsplit(uri)
        if parsed.scheme and parsed.scheme.lower() == "airflow":
            raise ValueError(f"{attr.name!r} scheme `airflow` is reserved")

    def __fspath__(self) -> str:
        return self.uri

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.uri == other.uri
        else:
            return NotImplemented

    def __hash__(self):
        return hash(self.uri)

    def iter_datasets(self) -> Iterator[tuple[str, Dataset]]:
        yield self.uri, self

    def evaluate(self, statuses: dict[str, bool]) -> bool:
        return statuses.get(self.uri, False)


class _DatasetBooleanCondition(BaseDatasetEventInput):
    """Base class for dataset boolean logic."""

    agg_func: Callable[[Iterable], bool]

    def __init__(self, *objects: BaseDatasetEventInput) -> None:
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


class DatasetAll(_DatasetBooleanCondition):
    """Use to combine datasets schedule references in an "or" relationship."""

    agg_func = all
