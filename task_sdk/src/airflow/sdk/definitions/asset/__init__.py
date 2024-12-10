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

import logging
import operator
import os
import urllib.parse
import warnings
from typing import TYPE_CHECKING, Any, Callable, ClassVar, NamedTuple, overload

import attrs

from airflow.serialization.dag_dependency import DagDependency

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator
    from urllib.parse import SplitResult

    from airflow.triggers.base import BaseTrigger


__all__ = [
    "Asset",
    "Dataset",
    "Model",
    "AssetRef",
    "AssetAlias",
    "AssetAll",
    "AssetAny",
]


log = logging.getLogger(__name__)


class AssetUniqueKey(NamedTuple):
    name: str
    uri: str

    @staticmethod
    def from_asset(asset: Asset) -> AssetUniqueKey:
        return AssetUniqueKey(name=asset.name, uri=asset.uri)


def normalize_noop(parts: SplitResult) -> SplitResult:
    """
    Place-hold a :class:`~urllib.parse.SplitResult`` normalizer.

    :meta private:
    """
    return parts


def _get_uri_normalizer(scheme: str) -> Callable[[SplitResult], SplitResult] | None:
    if scheme == "file":
        return normalize_noop
    from packaging.version import Version

    from airflow import __version__ as AIRFLOW_VERSION
    from airflow.providers_manager import ProvidersManager

    AIRFLOW_V_2 = Version(AIRFLOW_VERSION).base_version < Version("3.0.0").base_version
    if AIRFLOW_V_2:
        return ProvidersManager().dataset_uri_handlers.get(scheme)  # type: ignore[attr-defined]
    return ProvidersManager().asset_uri_handlers.get(scheme)


def _get_normalized_scheme(uri: str) -> str:
    parsed = urllib.parse.urlsplit(uri)
    return parsed.scheme.lower()


def _sanitize_uri(uri: str) -> str:
    """
    Sanitize an asset URI.

    This checks for URI validity, and normalizes the URI if needed. A fully
    normalized URI is returned.
    """
    parsed = urllib.parse.urlsplit(uri)
    if not parsed.scheme and not parsed.netloc:  # Does not look like a URI.
        return uri
    if not (normalized_scheme := _get_normalized_scheme(uri)):
        return uri
    if normalized_scheme.startswith("x-"):
        return uri
    if normalized_scheme == "airflow":
        raise ValueError("Asset scheme 'airflow' is reserved")
    _, auth_exists, normalized_netloc = parsed.netloc.rpartition("@")
    if auth_exists:
        # TODO: Collect this into a DagWarning.
        warnings.warn(
            "An Asset URI should not contain auth info (e.g. username or "
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


def _validate_identifier(instance, attribute, value):
    if not isinstance(value, str):
        raise ValueError(f"{type(instance).__name__} {attribute.name} must be a string")
    if len(value) > 1500:
        raise ValueError(f"{type(instance).__name__} {attribute.name} cannot exceed 1500 characters")
    if value.isspace():
        raise ValueError(f"{type(instance).__name__} {attribute.name} cannot be just whitespace")
    if not value.isascii():
        raise ValueError(f"{type(instance).__name__} {attribute.name} must only consist of ASCII characters")
    return value


def _validate_non_empty_identifier(instance, attribute, value):
    if not _validate_identifier(instance, attribute, value):
        raise ValueError(f"{type(instance).__name__} {attribute.name} cannot be empty")
    return value


def _validate_asset_name(instance, attribute, value):
    _validate_non_empty_identifier(instance, attribute, value)
    if value == "self" or value == "context":
        raise ValueError(f"prohibited name for asset: {value}")
    return value


def _set_extra_default(extra: dict | None) -> dict:
    """
    Automatically convert None to an empty dict.

    This allows the caller site to continue doing ``Asset(uri, extra=None)``,
    but still allow the ``extra`` attribute to always be a dict.
    """
    if extra is None:
        return {}
    return extra


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
        return AssetAny(self, other)

    def __and__(self, other: BaseAsset) -> BaseAsset:
        if not isinstance(other, BaseAsset):
            return NotImplemented
        return AssetAll(self, other)

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

    def iter_assets(self) -> Iterator[tuple[AssetUniqueKey, Asset]]:
        raise NotImplementedError

    def iter_asset_aliases(self) -> Iterator[tuple[str, AssetAlias]]:
        raise NotImplementedError

    def iter_dag_dependencies(self, *, source: str, target: str) -> Iterator[DagDependency]:
        """
        Iterate a base asset as dag dependency.

        :meta private:
        """
        raise NotImplementedError


@attrs.define(init=False, unsafe_hash=False)
class Asset(os.PathLike, BaseAsset):
    """A representation of data asset dependencies between workflows."""

    name: str = attrs.field(
        validator=[_validate_asset_name],
    )
    uri: str = attrs.field(
        validator=[_validate_non_empty_identifier],
        converter=_sanitize_uri,
    )
    group: str = attrs.field(
        default=attrs.Factory(operator.attrgetter("asset_type"), takes_self=True),
        validator=[_validate_identifier],
    )
    extra: dict[str, Any] = attrs.field(
        factory=dict,
        converter=_set_extra_default,
    )
    watchers: list[BaseTrigger] = attrs.field(
        factory=list,
    )

    asset_type: ClassVar[str] = "asset"
    __version__: ClassVar[int] = 1

    @overload
    def __init__(
        self,
        name: str,
        uri: str,
        *,
        group: str = ...,
        extra: dict | None = None,
        watchers: list[BaseTrigger] = ...,
    ) -> None:
        """Canonical; both name and uri are provided."""

    @overload
    def __init__(
        self,
        name: str,
        *,
        group: str = ...,
        extra: dict | None = None,
        watchers: list[BaseTrigger] = ...,
    ) -> None:
        """It's possible to only provide the name, either by keyword or as the only positional argument."""

    @overload
    def __init__(
        self,
        *,
        uri: str,
        group: str = ...,
        extra: dict | None = None,
        watchers: list[BaseTrigger] = ...,
    ) -> None:
        """It's possible to only provide the URI as a keyword argument."""

    def __init__(
        self,
        name: str | None = None,
        uri: str | None = None,
        *,
        group: str | None = None,
        extra: dict | None = None,
        watchers: list[BaseTrigger] | None = None,
    ) -> None:
        if name is None and uri is None:
            raise TypeError("Asset() requires either 'name' or 'uri'")
        elif name is None:
            name = uri
        elif uri is None:
            uri = name

        if TYPE_CHECKING:
            assert name is not None
            assert uri is not None

        # attrs default (and factory) does not kick in if any value is given to
        # the argument. We need to exclude defaults from the custom ___init___.
        kwargs: dict[str, Any] = {}
        if group is not None:
            kwargs["group"] = group
        if extra is not None:
            kwargs["extra"] = extra
        if watchers is not None:
            kwargs["watchers"] = watchers

        self.__attrs_init__(name=name, uri=uri, **kwargs)

    def __fspath__(self) -> str:
        return self.uri

    def __eq__(self, other: Any) -> bool:
        # The Asset class can be subclassed, and we don't want fields added by a
        # subclass to break equality. This explicitly filters out only fields
        # defined by the Asset class for comparison.
        if not isinstance(other, Asset):
            return NotImplemented
        f = attrs.filters.include(*attrs.fields_dict(Asset))
        return attrs.asdict(self, filter=f) == attrs.asdict(other, filter=f)

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
        Serialize the asset into its scheduling expression.

        :meta private:
        """
        return {"asset": {"uri": self.uri, "name": self.name, "group": self.group}}

    def iter_assets(self) -> Iterator[tuple[AssetUniqueKey, Asset]]:
        yield AssetUniqueKey.from_asset(self), self

    def iter_asset_aliases(self) -> Iterator[tuple[str, AssetAlias]]:
        return iter(())

    def evaluate(self, statuses: dict[str, bool]) -> bool:
        return statuses.get(self.uri, False)

    def iter_dag_dependencies(self, *, source: str, target: str) -> Iterator[DagDependency]:
        """
        Iterate an asset as dag dependency.

        :meta private:
        """
        yield DagDependency(
            source=source or "asset",
            target=target or "asset",
            dependency_type="asset",
            dependency_id=self.name,
        )


@attrs.define(kw_only=True)
class AssetRef:
    """Reference to an asset."""

    name: str


class Dataset(Asset):
    """A representation of dataset dependencies between workflows."""

    asset_type: ClassVar[str] = "dataset"


class Model(Asset):
    """A representation of model dependencies between workflows."""

    asset_type: ClassVar[str] = "model"


@attrs.define(unsafe_hash=False)
class AssetAlias(BaseAsset):
    """A representation of asset alias which is used to create asset during the runtime."""

    name: str = attrs.field(validator=_validate_non_empty_identifier)
    group: str = attrs.field(kw_only=True, default="asset", validator=_validate_identifier)

    def _resolve_assets(self) -> list[Asset]:
        from airflow.models.asset import expand_alias_to_assets
        from airflow.utils.session import create_session

        with create_session() as session:
            asset_models = expand_alias_to_assets(self.name, session)
        return [m.to_public() for m in asset_models]

    def as_expression(self) -> Any:
        """
        Serialize the asset alias into its scheduling expression.

        :meta private:
        """
        return {"alias": {"name": self.name, "group": self.group}}

    def evaluate(self, statuses: dict[str, bool]) -> bool:
        return any(x.evaluate(statuses=statuses) for x in self._resolve_assets())

    def iter_assets(self) -> Iterator[tuple[AssetUniqueKey, Asset]]:
        return iter(())

    def iter_asset_aliases(self) -> Iterator[tuple[str, AssetAlias]]:
        yield self.name, self

    def iter_dag_dependencies(self, *, source: str = "", target: str = "") -> Iterator[DagDependency]:
        """
        Iterate an asset alias and its resolved assets as dag dependency.

        :meta private:
        """
        if not (resolved_assets := self._resolve_assets()):
            yield DagDependency(
                source=source or "asset-alias",
                target=target or "asset-alias",
                dependency_type="asset-alias",
                dependency_id=self.name,
            )
            return
        for asset in resolved_assets:
            asset_name = asset.name
            # asset
            yield DagDependency(
                source=f"asset-alias:{self.name}" if source else "asset",
                target="asset" if source else f"asset-alias:{self.name}",
                dependency_type="asset",
                dependency_id=asset_name,
            )
            # asset alias
            yield DagDependency(
                source=source or f"asset:{asset_name}",
                target=target or f"asset:{asset_name}",
                dependency_type="asset-alias",
                dependency_id=self.name,
            )


class _AssetBooleanCondition(BaseAsset):
    """Base class for asset boolean logic."""

    agg_func: Callable[[Iterable], bool]

    def __init__(self, *objects: BaseAsset) -> None:
        if not all(isinstance(o, BaseAsset) for o in objects):
            raise TypeError("expect asset expressions in condition")
        self.objects = objects

    def evaluate(self, statuses: dict[str, bool]) -> bool:
        return self.agg_func(x.evaluate(statuses=statuses) for x in self.objects)

    def iter_assets(self) -> Iterator[tuple[AssetUniqueKey, Asset]]:
        seen: set[AssetUniqueKey] = set()  # We want to keep the first instance.
        for o in self.objects:
            for k, v in o.iter_assets():
                if k in seen:
                    continue
                yield k, v
                seen.add(k)

    def iter_asset_aliases(self) -> Iterator[tuple[str, AssetAlias]]:
        """Filter asset aliases in the condition."""
        seen: set[str] = set()  # We want to keep the first instance.
        for o in self.objects:
            for k, v in o.iter_asset_aliases():
                if k in seen:
                    continue
                yield k, v
                seen.add(k)

    def iter_dag_dependencies(self, *, source: str, target: str) -> Iterator[DagDependency]:
        """
        Iterate asset, asset aliases and their resolved assets  as dag dependency.

        :meta private:
        """
        for obj in self.objects:
            yield from obj.iter_dag_dependencies(source=source, target=target)


class AssetAny(_AssetBooleanCondition):
    """Use to combine assets schedule references in an "and" relationship."""

    agg_func = any

    def __or__(self, other: BaseAsset) -> BaseAsset:
        if not isinstance(other, BaseAsset):
            return NotImplemented
        # Optimization: X | (Y | Z) is equivalent to X | Y | Z.
        return AssetAny(*self.objects, other)

    def __repr__(self) -> str:
        return f"AssetAny({', '.join(map(str, self.objects))})"

    def as_expression(self) -> dict[str, Any]:
        """
        Serialize the asset into its scheduling expression.

        :meta private:
        """
        return {"any": [o.as_expression() for o in self.objects]}


class AssetAll(_AssetBooleanCondition):
    """Use to combine assets schedule references in an "or" relationship."""

    agg_func = all

    def __and__(self, other: BaseAsset) -> BaseAsset:
        if not isinstance(other, BaseAsset):
            return NotImplemented
        # Optimization: X & (Y & Z) is equivalent to X & Y & Z.
        return AssetAll(*self.objects, other)

    def __repr__(self) -> str:
        return f"AssetAll({', '.join(map(str, self.objects))})"

    def as_expression(self) -> Any:
        """
        Serialize the assets into its scheduling expression.

        :meta private:
        """
        return {"all": [o.as_expression() for o in self.objects]}
