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
from typing import TYPE_CHECKING, Any, Callable, ClassVar, Iterable, Iterator, cast

import attr
from sqlalchemy import select

from airflow.api_internal.internal_api_call import internal_api_call
from airflow.serialization.dag_dependency import DagDependency
from airflow.typing_compat import TypedDict
from airflow.utils.session import NEW_SESSION, provide_session

if TYPE_CHECKING:
    from urllib.parse import SplitResult

    from sqlalchemy.orm.session import Session


from airflow.configuration import conf

__all__ = ["Dataset", "DatasetAll", "DatasetAny"]


def normalize_noop(parts: SplitResult) -> SplitResult:
    """
    Place-hold a :class:`~urllib.parse.SplitResult`` normalizer.

    :meta private:
    """
    return parts


def _get_uri_normalizer(scheme: str) -> Callable[[SplitResult], SplitResult] | None:
    if scheme == "file":
        return normalize_noop
    from airflow.providers_manager import ProvidersManager

    return ProvidersManager().dataset_uri_handlers.get(scheme)


def _get_normalized_scheme(uri: str) -> str:
    parsed = urllib.parse.urlsplit(uri)
    return parsed.scheme.lower()


def _sanitize_uri(uri: str) -> str:
    """
    Sanitize a dataset URI.

    This checks for URI validity, and normalizes the URI if needed. A fully
    normalized URI is returned.
    """
    if not uri:
        raise ValueError("Dataset URI cannot be empty")
    if uri.isspace():
        raise ValueError("Dataset URI cannot be just whitespace")
    if not uri.isascii():
        raise ValueError("Dataset URI must only consist of ASCII characters")
    parsed = urllib.parse.urlsplit(uri)
    if not parsed.scheme and not parsed.netloc:  # Does not look like a URI.
        return uri
    if not (normalized_scheme := _get_normalized_scheme(uri)):
        return uri
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
        try:
            parsed = normalizer(parsed)
        except ValueError as exception:
            if conf.getboolean("core", "strict_dataset_uri_validation", fallback=False):
                raise
            warnings.warn(
                f"The dataset URI {uri} is not AIP-60 compliant: {exception}. "
                f"In Airflow 3, this will raise an exception.",
                UserWarning,
                stacklevel=3,
            )
    return urllib.parse.urlunsplit(parsed)


def extract_event_key(value: str | Dataset | DatasetAlias) -> str:
    """
    Extract the key of an inlet or an outlet event.

    If the input value is a string, it is treated as a URI and sanitized. If the
    input is a :class:`Dataset`, the URI it contains is considered sanitized and
    returned directly. If the input is a :class:`DatasetAlias`, the name it contains
    will be returned directly.

    :meta private:
    """
    if isinstance(value, DatasetAlias):
        return value.name

    if isinstance(value, Dataset):
        return value.uri
    return _sanitize_uri(str(value))


@internal_api_call
@provide_session
def expand_alias_to_datasets(alias: str | DatasetAlias, *, session: Session = NEW_SESSION) -> list[BaseAsset]:
    """Expand dataset alias to resolved datasets."""
    from airflow.models.dataset import DatasetAliasModel

    alias_name = alias.name if isinstance(alias, DatasetAlias) else alias

    dataset_alias_obj = session.scalar(
        select(DatasetAliasModel).where(DatasetAliasModel.name == alias_name).limit(1)
    )
    if dataset_alias_obj:
        return [Dataset(uri=dataset.uri, extra=dataset.extra) for dataset in dataset_alias_obj.datasets]
    return []


class BaseAsset:
    """
    Protocol for all asset triggers to use in ``DAG(schedule=...)``.

    :meta private:
    """

    def __bool__(self) -> bool:
        return True

    def __or__(self, other: BaseAsset) -> BaseAsset:
        if not isinstance(other, BaseAsset):
            return NotImplemented
        return DatasetAny(self, other)

    def __and__(self, other: BaseAsset) -> BaseAsset:
        if not isinstance(other, BaseAsset):
            return NotImplemented
        return DatasetAll(self, other)

    def as_expression(self) -> Any:
        """
        Serialize the asset into its scheduling expression.

        The return value is stored in DagModel for display purposes. It must be
        JSON-compatible.

        :meta private:
        """
        raise NotImplementedError

    def evaluate(self, statuses: dict[str, bool]) -> bool:
        raise NotImplementedError

    def iter_datasets(self) -> Iterator[tuple[str, Dataset]]:
        raise NotImplementedError

    def iter_dataset_aliases(self) -> Iterator[tuple[str, DatasetAlias]]:
        raise NotImplementedError

    def iter_dag_dependencies(self, *, source: str, target: str) -> Iterator[DagDependency]:
        """
        Iterate a base dataset as dag dependency.

        :meta private:
        """
        raise NotImplementedError


@attr.define(unsafe_hash=False)
class DatasetAlias(BaseAsset):
    """A represeation of dataset alias which is used to create dataset during the runtime."""

    name: str

    def iter_datasets(self) -> Iterator[tuple[str, Dataset]]:
        return iter(())

    def iter_dataset_aliases(self) -> Iterator[tuple[str, DatasetAlias]]:
        yield self.name, self

    def iter_dag_dependencies(self, *, source: str, target: str) -> Iterator[DagDependency]:
        """
        Iterate a dataset alias as dag dependency.

        :meta private:
        """
        yield DagDependency(
            source=source or "dataset-alias",
            target=target or "dataset-alias",
            dependency_type="dataset-alias",
            dependency_id=self.name,
        )


class DatasetAliasEvent(TypedDict):
    """A represeation of dataset event to be triggered by a dataset alias."""

    source_alias_name: str
    dest_dataset_uri: str
    extra: dict[str, Any]


def _set_extra_default(extra: dict | None) -> dict:
    """
    Automatically convert None to an empty dict.

    This allows the caller site to continue doing ``Dataset(uri, extra=None)``,
    but still allow the ``extra`` attribute to always be a dict.
    """
    if extra is None:
        return {}
    return extra


@attr.define(unsafe_hash=False)
class Dataset(os.PathLike, BaseAsset):
    """A representation of data dependencies between workflows."""

    uri: str = attr.field(
        converter=_sanitize_uri,
        validator=[attr.validators.min_len(1), attr.validators.max_len(3000)],
    )
    extra: dict[str, Any] = attr.field(factory=dict, converter=_set_extra_default)

    __version__: ClassVar[int] = 1

    def __fspath__(self) -> str:
        return self.uri

    @property
    def normalized_uri(self) -> str | None:
        """
        Returns the normalized and AIP-60 compliant URI whenever possible.

        If we can't retrieve the scheme from URI or no normalizer is provided or if parsing fails,
        it returns None.

        If a normalizer for the scheme exists and parsing is successful we return the normalizer result.
        """
        if not (normalized_scheme := _get_normalized_scheme(self.uri)):
            return None

        if (normalizer := _get_uri_normalizer(normalized_scheme)) is None:
            return None
        parsed = urllib.parse.urlsplit(self.uri)
        try:
            normalized_uri = normalizer(parsed)
            return urllib.parse.urlunsplit(normalized_uri)
        except ValueError:
            return None

    def as_expression(self) -> Any:
        """
        Serialize the dataset into its scheduling expression.

        :meta private:
        """
        return self.uri

    def iter_datasets(self) -> Iterator[tuple[str, Dataset]]:
        yield self.uri, self

    def iter_dataset_aliases(self) -> Iterator[tuple[str, DatasetAlias]]:
        return iter(())

    def evaluate(self, statuses: dict[str, bool]) -> bool:
        return statuses.get(self.uri, False)

    def iter_dag_dependencies(self, *, source: str, target: str) -> Iterator[DagDependency]:
        """
        Iterate a dataset as dag dependency.

        :meta private:
        """
        yield DagDependency(
            source=source or "dataset",
            target=target or "dataset",
            dependency_type="dataset",
            dependency_id=self.uri,
        )


class _AssetBooleanCondition(BaseAsset):
    """Base class for asset boolean logic."""

    agg_func: Callable[[Iterable], bool]

    def __init__(self, *objects: BaseAsset) -> None:
        if not all(isinstance(o, BaseAsset) for o in objects):
            raise TypeError("expect asset expressions in condition")

        self.objects = [
            _DatasetAliasCondition(obj.name) if isinstance(obj, DatasetAlias) else obj for obj in objects
        ]

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

    def iter_dataset_aliases(self) -> Iterator[tuple[str, DatasetAlias]]:
        """Filter dataest aliases in the condition."""
        for o in self.objects:
            yield from o.iter_dataset_aliases()

    def iter_dag_dependencies(self, *, source: str, target: str) -> Iterator[DagDependency]:
        """
        Iterate dataset, dataset aliases and their resolved datasets  as dag dependency.

        :meta private:
        """
        for obj in self.objects:
            yield from obj.iter_dag_dependencies(source=source, target=target)


class DatasetAny(_AssetBooleanCondition):
    """Use to combine datasets schedule references in an "and" relationship."""

    agg_func = any

    def __or__(self, other: BaseAsset) -> BaseAsset:
        if not isinstance(other, BaseAsset):
            return NotImplemented
        # Optimization: X | (Y | Z) is equivalent to X | Y | Z.
        return DatasetAny(*self.objects, other)

    def __repr__(self) -> str:
        return f"DatasetAny({', '.join(map(str, self.objects))})"

    def as_expression(self) -> dict[str, Any]:
        """
        Serialize the dataset into its scheduling expression.

        :meta private:
        """
        return {"any": [o.as_expression() for o in self.objects]}


class _DatasetAliasCondition(DatasetAny):
    """
    Use to expand DataAlias as DatasetAny of its resolved Datasets.

    :meta private:
    """

    def __init__(self, name: str) -> None:
        self.name = name
        self.objects = expand_alias_to_datasets(name)

    def __repr__(self) -> str:
        return f"_DatasetAliasCondition({', '.join(map(str, self.objects))})"

    def as_expression(self) -> Any:
        """
        Serialize the dataset into its scheduling expression.

        :meta private:
        """
        return {"alias": self.name}

    def iter_dataset_aliases(self) -> Iterator[tuple[str, DatasetAlias]]:
        yield self.name, DatasetAlias(self.name)

    def iter_dag_dependencies(self, *, source: str = "", target: str = "") -> Iterator[DagDependency]:
        """
        Iterate a dataset alias and its resolved datasets  as dag dependency.

        :meta private:
        """
        if self.objects:
            for obj in self.objects:
                dataset = cast(Dataset, obj)
                uri = dataset.uri
                # dataset
                yield DagDependency(
                    source=f"dataset-alias:{self.name}" if source else "dataset",
                    target="dataset" if source else f"dataset-alias:{self.name}",
                    dependency_type="dataset",
                    dependency_id=uri,
                )
                # dataset alias
                yield DagDependency(
                    source=source or f"dataset:{uri}",
                    target=target or f"dataset:{uri}",
                    dependency_type="dataset-alias",
                    dependency_id=self.name,
                )
        else:
            yield DagDependency(
                source=source or "dataset-alias",
                target=target or "dataset-alias",
                dependency_type="dataset-alias",
                dependency_id=self.name,
            )


class DatasetAll(_AssetBooleanCondition):
    """Use to combine datasets schedule references in an "or" relationship."""

    agg_func = all

    def __and__(self, other: BaseAsset) -> BaseAsset:
        if not isinstance(other, BaseAsset):
            return NotImplemented
        # Optimization: X & (Y & Z) is equivalent to X & Y & Z.
        return DatasetAll(*self.objects, other)

    def __repr__(self) -> str:
        return f"DatasetAll({', '.join(map(str, self.objects))})"

    def as_expression(self) -> Any:
        """
        Serialize the dataset into its scheduling expression.

        :meta private:
        """
        return {"all": [o.as_expression() for o in self.objects]}
