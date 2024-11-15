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

import inspect
import logging
import os
import urllib.parse
import warnings
from collections.abc import Iterable, Iterator, Mapping
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    cast,
    overload,
)

import attrs
from sqlalchemy import select

from airflow.api_internal.internal_api_call import internal_api_call
from airflow.models.asset import _fetch_active_assets_by_name
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk.definitions.dag import DAG, ScheduleArg
from airflow.serialization.dag_dependency import DagDependency
from airflow.typing_compat import TypedDict
from airflow.utils.session import NEW_SESSION, create_session, provide_session

if TYPE_CHECKING:
    from urllib.parse import SplitResult

    from sqlalchemy.orm.session import Session

    from airflow.io.path import ObjectStoragePath

__all__ = ["Asset", "AssetAll", "AssetAny", "Dataset", "Model", "AssetRef", "asset"]


log = logging.getLogger(__name__)


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


def extract_event_key(value: str | Asset | AssetAlias) -> str:
    """
    Extract the key of an inlet or an outlet event.

    If the input value is a string, it is treated as a URI and sanitized. If the
    input is a :class:`Asset`, the URI it contains is considered sanitized and
    returned directly. If the input is a :class:`AssetAlias`, the name it contains
    will be returned directly.

    :meta private:
    """
    if isinstance(value, AssetAlias):
        return value.name

    if isinstance(value, Asset):
        return value.uri
    return _sanitize_uri(str(value))


@internal_api_call
@provide_session
def expand_alias_to_assets(alias: str | AssetAlias, *, session: Session = NEW_SESSION) -> list[BaseAsset]:
    """Expand asset alias to resolved assets."""
    from airflow.models.asset import AssetAliasModel

    alias_name = alias.name if isinstance(alias, AssetAlias) else alias

    asset_alias_obj = session.scalar(
        select(AssetAliasModel).where(AssetAliasModel.name == alias_name).limit(1)
    )
    if asset_alias_obj:
        return [asset.to_public() for asset in asset_alias_obj.assets]
    return []


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

    def iter_assets(self) -> Iterator[tuple[str, Asset]]:
        raise NotImplementedError

    def iter_asset_aliases(self) -> Iterator[tuple[str, AssetAlias]]:
        raise NotImplementedError

    def iter_dag_dependencies(self, *, source: str, target: str) -> Iterator[DagDependency]:
        """
        Iterate a base asset as dag dependency.

        :meta private:
        """
        raise NotImplementedError


@attrs.define(unsafe_hash=False)
class AssetAlias(BaseAsset):
    """A represeation of asset alias which is used to create asset during the runtime."""

    name: str = attrs.field(validator=_validate_non_empty_identifier)
    group: str = attrs.field(kw_only=True, default="", validator=_validate_identifier)

    def iter_assets(self) -> Iterator[tuple[str, Asset]]:
        return iter(())

    def iter_asset_aliases(self) -> Iterator[tuple[str, AssetAlias]]:
        yield self.name, self

    def iter_dag_dependencies(self, *, source: str, target: str) -> Iterator[DagDependency]:
        """
        Iterate an asset alias as dag dependency.

        :meta private:
        """
        yield DagDependency(
            source=source or "asset-alias",
            target=target or "asset-alias",
            dependency_type="asset-alias",
            dependency_id=self.name,
        )


class AssetAliasEvent(TypedDict):
    """A represeation of asset event to be triggered by an asset alias."""

    source_alias_name: str
    dest_asset_uri: str
    extra: dict[str, Any]


@attrs.define(init=False, unsafe_hash=False)
class Asset(os.PathLike, BaseAsset):
    """A representation of data asset dependencies between workflows."""

    name: str
    uri: str
    group: str
    extra: dict[str, Any]

    asset_type: ClassVar[str] = "asset"
    __version__: ClassVar[int] = 1

    @overload
    def __init__(self, name: str, uri: str, *, group: str = "", extra: dict | None = None) -> None:
        """Canonical; both name and uri are provided."""

    @overload
    def __init__(self, name: str, *, group: str = "", extra: dict | None = None) -> None:
        """It's possible to only provide the name, either by keyword or as the only positional argument."""

    @overload
    def __init__(self, *, uri: str, group: str = "", extra: dict | None = None) -> None:
        """It's possible to only provide the URI as a keyword argument."""

    def __init__(
        self,
        name: str | None = None,
        uri: str | None = None,
        *,
        group: str = "",
        extra: dict | None = None,
    ) -> None:
        if name is None and uri is None:
            raise TypeError("Asset() requires either 'name' or 'uri'")
        elif name is None:
            name = uri
        elif uri is None:
            uri = name
        fields = attrs.fields_dict(Asset)
        self.name = _validate_asset_name(self, fields["name"], name)
        self.uri = _sanitize_uri(_validate_non_empty_identifier(self, fields["uri"], uri))
        self.group = _validate_identifier(self, fields["group"], group) if group else self.asset_type
        self.extra = _set_extra_default(extra)

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
        Serialize the asset into its scheduling expression.

        :meta private:
        """
        return self.uri

    def iter_assets(self) -> Iterator[tuple[str, Asset]]:
        yield self.uri, self

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
            dependency_id=self.uri,
        )


class Dataset(Asset):
    """A representation of dataset dependencies between workflows."""

    asset_type: ClassVar[str] = "dataset"


class Model(Asset):
    """A representation of model dependencies between workflows."""

    asset_type: ClassVar[str] = "model"


class _AssetBooleanCondition(BaseAsset):
    """Base class for asset boolean logic."""

    agg_func: Callable[[Iterable], bool]

    def __init__(self, *objects: BaseAsset) -> None:
        if not all(isinstance(o, BaseAsset) for o in objects):
            raise TypeError("expect asset expressions in condition")

        self.objects = [
            AssetAliasCondition(obj.name) if isinstance(obj, AssetAlias) else obj for obj in objects
        ]

    def evaluate(self, statuses: dict[str, bool]) -> bool:
        return self.agg_func(x.evaluate(statuses=statuses) for x in self.objects)

    def iter_assets(self) -> Iterator[tuple[str, Asset]]:
        seen = set()  # We want to keep the first instance.
        for o in self.objects:
            for k, v in o.iter_assets():
                if k in seen:
                    continue
                yield k, v
                seen.add(k)

    def iter_asset_aliases(self) -> Iterator[tuple[str, AssetAlias]]:
        """Filter asset aliases in the condition."""
        for o in self.objects:
            yield from o.iter_asset_aliases()

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


class AssetAliasCondition(AssetAny):
    """
    Use to expand AssetAlias as AssetAny of its resolved Assets.

    :meta private:
    """

    def __init__(self, name: str) -> None:
        self.name = name
        self.objects = expand_alias_to_assets(name)

    def __repr__(self) -> str:
        return f"AssetAliasCondition({', '.join(map(str, self.objects))})"

    def as_expression(self) -> Any:
        """
        Serialize the asset alias into its scheduling expression.

        :meta private:
        """
        return {"alias": self.name}

    def iter_asset_aliases(self) -> Iterator[tuple[str, AssetAlias]]:
        yield self.name, AssetAlias(self.name)

    def iter_dag_dependencies(self, *, source: str = "", target: str = "") -> Iterator[DagDependency]:
        """
        Iterate an asset alias and its resolved assets as dag dependency.

        :meta private:
        """
        if self.objects:
            for obj in self.objects:
                asset = cast(Asset, obj)
                uri = asset.uri
                # asset
                yield DagDependency(
                    source=f"asset-alias:{self.name}" if source else "asset",
                    target="asset" if source else f"asset-alias:{self.name}",
                    dependency_type="asset",
                    dependency_id=uri,
                )
                # asset alias
                yield DagDependency(
                    source=source or f"asset:{uri}",
                    target=target or f"asset:{uri}",
                    dependency_type="asset-alias",
                    dependency_id=self.name,
                )
        else:
            yield DagDependency(
                source=source or "asset-alias",
                target=target or "asset-alias",
                dependency_type="asset-alias",
                dependency_id=self.name,
            )


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


@attrs.define(init=False)
class Metadata:
    """Metadata to attach to an AssetEvent."""

    uri: str
    extra: dict[str, Any]
    alias_name: str | None = None

    def __init__(
        self,
        target: str | Asset,
        extra: dict[str, Any],
        alias: AssetAlias | str | None = None,
    ) -> None:
        self.uri = extract_event_key(target)
        self.extra = extra
        if isinstance(alias, AssetAlias):
            self.alias_name = alias.name
        else:
            self.alias_name = alias


@attrs.define(kw_only=True)
class AssetRef:
    """Reference to an asset."""

    name: str


class _AssetMainOperator(PythonOperator):
    def __init__(self, *, definition_name: str, uri: str | None = None, **kwargs) -> None:
        super().__init__(**kwargs)
        self._definition_name = definition_name
        self._uri = uri

    def _iter_kwargs(
        self, context: Mapping[str, Any], active_assets: dict[str, Asset]
    ) -> Iterator[tuple[str, Any]]:
        value: Any
        for key in inspect.signature(self.python_callable).parameters:
            if key == "self":
                value = active_assets.get(self._definition_name)
            elif key == "context":
                value = context
            else:
                value = active_assets.get(key, Asset(name=key))
            yield key, value

    def determine_kwargs(self, context: Mapping[str, Any]) -> Mapping[str, Any]:
        active_assets: dict[str, Asset] = {}
        asset_names = [asset_ref.name for asset_ref in self.inlets if isinstance(asset_ref, AssetRef)]
        if "self" in inspect.signature(self.python_callable).parameters:
            asset_names.append(self._definition_name)

        if asset_names:
            with create_session() as session:
                active_assets = _fetch_active_assets_by_name(asset_names, session)
        return dict(self._iter_kwargs(context, active_assets))


@attrs.define(kw_only=True)
class AssetDefinition(Asset):
    """
    Asset representation from decorating a function with ``@asset``.

    :meta private:
    """

    function: Callable
    schedule: ScheduleArg

    def __attrs_post_init__(self) -> None:
        parameters = inspect.signature(self.function).parameters

        with DAG(dag_id=self.name, schedule=self.schedule, auto_register=True):
            _AssetMainOperator(
                task_id="__main__",
                inlets=[
                    AssetRef(name=inlet_asset_name)
                    for inlet_asset_name in parameters
                    if inlet_asset_name not in ("self", "context")
                ],
                outlets=[self.to_asset()],
                python_callable=self.function,
                definition_name=self.name,
                uri=self.uri,
            )

    def to_asset(self) -> Asset:
        return Asset(
            name=self.name,
            uri=self.uri,
            group=self.group,
            extra=self.extra,
        )

    def serialize(self):
        return {
            "uri": self.uri,
            "name": self.name,
            "group": self.group,
            "extra": self.extra,
        }


@attrs.define(kw_only=True)
class asset:
    """Create an asset by decorating a materialization function."""

    schedule: ScheduleArg
    uri: str | ObjectStoragePath | None = None
    group: str = ""
    extra: dict[str, Any] = attrs.field(factory=dict)

    def __call__(self, f: Callable) -> AssetDefinition:
        if (name := f.__name__) != f.__qualname__:
            raise ValueError("nested function not supported")

        return AssetDefinition(
            name=name,
            uri=name if self.uri is None else str(self.uri),
            group=self.group,
            extra=self.extra,
            function=f,
            schedule=self.schedule,
        )
