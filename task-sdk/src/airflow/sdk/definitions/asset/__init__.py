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

import json
import logging
import operator
import os
import urllib.parse
import warnings
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, ClassVar, Literal, overload

import attrs

from airflow.sdk.api.datamodels._generated import AssetProfile
from airflow.serialization.dag_dependency import DagDependency

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator
    from urllib.parse import SplitResult

    from pydantic.types import JsonValue

    from airflow.models.asset import AssetModel
    from airflow.sdk.io.path import ObjectStoragePath
    from airflow.serialization.serialized_objects import SerializedAssetWatcher
    from airflow.triggers.base import BaseEventTrigger

    AttrsInstance = attrs.AttrsInstance
else:
    AttrsInstance = object


__all__ = [
    "Asset",
    "Dataset",
    "Model",
    "AssetAlias",
    "AssetAll",
    "AssetAny",
    "AssetNameRef",
    "AssetRef",
    "AssetUriRef",
    "AssetWatcher",
]

from airflow.sdk.configuration import conf

log = logging.getLogger(__name__)


SQL_ALCHEMY_CONN = conf.get("database", "SQL_ALCHEMY_CONN", fallback="NOT AVAILABLE")


@attrs.define(frozen=True)
class AssetUniqueKey(attrs.AttrsInstance):
    """
    Columns to identify an unique asset.

    :meta private:
    """

    name: str
    uri: str

    @staticmethod
    def from_asset(asset: Asset | AssetModel) -> AssetUniqueKey:
        return AssetUniqueKey(name=asset.name, uri=asset.uri)

    def to_asset(self) -> Asset:
        return Asset(name=self.name, uri=self.uri)

    @staticmethod
    def from_str(key: str) -> AssetUniqueKey:
        return AssetUniqueKey(**json.loads(key))

    def to_str(self) -> str:
        return json.dumps(attrs.asdict(self))

    @staticmethod
    def from_profile(profile: AssetProfile) -> AssetUniqueKey:
        if profile.name and profile.uri:
            return AssetUniqueKey(name=profile.name, uri=profile.uri)

        if name := profile.name:
            return AssetUniqueKey(name=name, uri=name)
        if uri := profile.uri:
            return AssetUniqueKey(name=uri, uri=uri)

        raise ValueError("name and uri cannot both be empty")


@attrs.define(frozen=True)
class AssetAliasUniqueKey:
    """
    Columns to identify an unique asset alias.

    :meta private:
    """

    name: str

    @staticmethod
    def from_asset_alias(asset_alias: AssetAlias) -> AssetAliasUniqueKey:
        return AssetAliasUniqueKey(name=asset_alias.name)

    def to_asset_alias(self) -> AssetAlias:
        return AssetAlias(name=self.name)


BaseAssetUniqueKey = AssetUniqueKey | AssetAliasUniqueKey


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

    return ProvidersManager().asset_uri_handlers.get(scheme)


def _get_normalized_scheme(uri: str) -> str:
    parsed = urllib.parse.urlsplit(uri)
    return parsed.scheme.lower()


def _sanitize_uri(inp: str | ObjectStoragePath) -> str:
    """
    Sanitize an asset URI.

    This checks for URI validity, and normalizes the URI if needed. A fully
    normalized URI is returned.
    """
    uri = str(inp)
    parsed = urllib.parse.urlsplit(uri)
    if not parsed.scheme and not parsed.netloc:  # Does not look like a URI.
        return uri
    if not (normalized_scheme := _get_normalized_scheme(uri)):
        return uri
    if normalized_scheme.startswith("x-"):
        return uri
    if normalized_scheme == "airflow":
        raise ValueError("Asset scheme 'airflow' is reserved")
    if parsed.password:
        # TODO: Collect this into a DagWarning.
        warnings.warn(
            "An Asset URI should not contain a password. User info has been automatically dropped.",
            UserWarning,
            stacklevel=3,
        )
        _, _, normalized_netloc = parsed.netloc.rpartition("@")
    else:
        normalized_netloc = parsed.netloc
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
    # We use latin1_general_cs to store the name (and group, asset values etc.) on MySQL.
    # relaxing this check for non mysql backend
    if SQL_ALCHEMY_CONN.startswith("mysql") and not value.isascii():
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


def _set_extra_default(extra: dict[str, JsonValue] | None) -> dict:
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

    def iter_assets(self) -> Iterator[tuple[AssetUniqueKey, Asset]]:
        raise NotImplementedError

    def iter_asset_aliases(self) -> Iterator[tuple[str, AssetAlias]]:
        raise NotImplementedError

    def iter_asset_refs(self) -> Iterator[AssetRef]:
        raise NotImplementedError

    def iter_dag_dependencies(self, *, source: str, target: str) -> Iterator[DagDependency]:
        """
        Iterate a base asset as dag dependency.

        :meta private:
        """
        raise NotImplementedError


@attrs.define(init=False)
class AssetWatcher:
    """A representation of an asset watcher. The name uniquely identifies the watch."""

    name: str
    # This attribute serves double purpose.
    # For a "normal" asset instance loaded from Dag, this holds the trigger used to monitor an external
    # resource. In that case, ``AssetWatcher`` is used directly by users.
    # For an asset recreated from a serialized Dag, this holds the serialized data of the trigger. In that
    # case, `SerializedAssetWatcher` is used. We need to keep the two types to make mypy happy because
    # `SerializedAssetWatcher` is a subclass of `AssetWatcher`.
    trigger: BaseEventTrigger | dict

    def __init__(
        self,
        name: str,
        trigger: BaseEventTrigger | dict,
    ) -> None:
        from airflow.triggers.base import BaseEventTrigger, BaseTrigger

        if isinstance(trigger, BaseTrigger) and not isinstance(trigger, BaseEventTrigger):
            raise ValueError("The trigger used to watch an asset must inherit ``BaseEventTrigger``")

        self.name = name
        self.trigger = trigger


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
    extra: dict[str, JsonValue] = attrs.field(
        factory=dict,
        converter=_set_extra_default,
    )
    watchers: list[AssetWatcher | SerializedAssetWatcher] = attrs.field(
        factory=list,
    )

    asset_type: ClassVar[str] = "asset"
    __version__: ClassVar[int] = 1

    @overload
    def __init__(
        self,
        name: str,
        uri: str | ObjectStoragePath,
        *,
        group: str = ...,
        extra: dict[str, JsonValue] | None = None,
        watchers: list[AssetWatcher | SerializedAssetWatcher] = ...,
    ) -> None:
        """Canonical; both name and uri are provided."""

    @overload
    def __init__(
        self,
        name: str,
        *,
        group: str = ...,
        extra: dict[str, JsonValue] | None = None,
        watchers: list[AssetWatcher | SerializedAssetWatcher] = ...,
    ) -> None:
        """It's possible to only provide the name, either by keyword or as the only positional argument."""

    @overload
    def __init__(
        self,
        *,
        uri: str | ObjectStoragePath,
        group: str = ...,
        extra: dict[str, JsonValue] | None = None,
        watchers: list[AssetWatcher | SerializedAssetWatcher] = ...,
    ) -> None:
        """It's possible to only provide the URI as a keyword argument."""

    def __init__(
        self,
        name: str | None = None,
        uri: str | ObjectStoragePath | None = None,
        *,
        group: str | None = None,
        extra: dict[str, JsonValue] | None = None,
        watchers: list[AssetWatcher | SerializedAssetWatcher] | None = None,
    ) -> None:
        if name is None and uri is None:
            raise TypeError("Asset() requires either 'name' or 'uri'")
        if name is None:
            name = str(uri)
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

    @overload
    @staticmethod
    def ref(*, name: str) -> AssetNameRef: ...

    @overload
    @staticmethod
    def ref(*, uri: str) -> AssetUriRef: ...

    @staticmethod
    def ref(*, name: str = "", uri: str = "") -> AssetRef:
        if name and uri:
            raise TypeError("Asset reference must be made to either name or URI, not both")
        if name:
            return AssetNameRef(name)
        if uri:
            return AssetUriRef(uri)
        raise TypeError("Asset reference expects keyword argument 'name' or 'uri'")

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

    def __hash__(self):
        f = attrs.filters.include(*attrs.fields_dict(Asset))
        return hash(attrs.asdict(self, filter=f))

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

    def iter_asset_refs(self) -> Iterator[AssetRef]:
        return iter(())

    def iter_dag_dependencies(self, *, source: str, target: str) -> Iterator[DagDependency]:
        """
        Iterate an asset as dag dependency.

        :meta private:
        """
        yield DagDependency(
            source=source or "asset",
            target=target or "asset",
            label=self.name,
            dependency_type="asset",
            # We can't get asset id at this stage.
            # This will be updated when running SerializedDagModel.get_dag_dependencies
            dependency_id=AssetUniqueKey.from_asset(self).to_str(),
        )

    def asprofile(self) -> AssetProfile:
        """
        Profiles Asset to AssetProfile.

        :meta private:
        """
        return AssetProfile(name=self.name or None, uri=self.uri or None, type=Asset.__name__)


class AssetRef(BaseAsset, AttrsInstance):
    """
    Reference to an asset.

    This class is not intended to be instantiated directly. Call ``Asset.ref``
    instead to create one of the subclasses.

    :meta private:
    """

    _dependency_type: Literal["asset-name-ref", "asset-uri-ref"]

    def as_expression(self) -> Any:
        return {"asset_ref": attrs.asdict(self)}

    def iter_assets(self) -> Iterator[tuple[AssetUniqueKey, Asset]]:
        return iter(())

    def iter_asset_aliases(self) -> Iterator[tuple[str, AssetAlias]]:
        return iter(())

    def iter_asset_refs(self) -> Iterator[AssetRef]:
        yield self

    def iter_dag_dependencies(self, *, source: str = "", target: str = "") -> Iterator[DagDependency]:
        (dependency_id,) = attrs.astuple(self)
        yield DagDependency(
            source=source or self._dependency_type,
            target=target or self._dependency_type,
            label=dependency_id,
            dependency_type=self._dependency_type,
            dependency_id=dependency_id,
        )


@attrs.define(hash=True)
class AssetNameRef(AssetRef):
    """Name reference to an asset."""

    name: str

    _dependency_type = "asset-name-ref"


@attrs.define(hash=True)
class AssetUriRef(AssetRef):
    """URI reference to an asset."""

    uri: str

    _dependency_type = "asset-uri-ref"


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

    def as_expression(self) -> Any:
        """
        Serialize the asset alias into its scheduling expression.

        :meta private:
        """
        return {"alias": {"name": self.name, "group": self.group}}

    def iter_assets(self) -> Iterator[tuple[AssetUniqueKey, Asset]]:
        return iter(())

    def iter_asset_aliases(self) -> Iterator[tuple[str, AssetAlias]]:
        yield self.name, self

    def iter_asset_refs(self) -> Iterator[AssetRef]:
        return iter(())

    def iter_dag_dependencies(self, *, source: str = "", target: str = "") -> Iterator[DagDependency]:
        """
        Iterate an asset alias and its resolved assets as dag dependency.

        :meta private:
        """
        yield DagDependency(
            source=source or "asset-alias",
            target=target or "asset-alias",
            label=self.name,
            dependency_type="asset-alias",
            dependency_id=self.name,
        )


class AssetBooleanCondition(BaseAsset):
    """
    Base class for asset boolean logic.

    :meta private:
    """

    agg_func: Callable[[Iterable], bool]

    def __init__(self, *objects: BaseAsset) -> None:
        if not all(isinstance(o, BaseAsset) for o in objects):
            raise TypeError("expect asset expressions in condition")
        self.objects = objects

    def iter_assets(self) -> Iterator[tuple[AssetUniqueKey, Asset]]:
        for o in self.objects:
            yield from o.iter_assets()

    def iter_asset_aliases(self) -> Iterator[tuple[str, AssetAlias]]:
        for o in self.objects:
            yield from o.iter_asset_aliases()

    def iter_asset_refs(self) -> Iterator[AssetRef]:
        for o in self.objects:
            yield from o.iter_asset_refs()

    def iter_dag_dependencies(self, *, source: str, target: str) -> Iterator[DagDependency]:
        """
        Iterate asset, asset aliases and their resolved assets  as dag dependency.

        :meta private:
        """
        for obj in self.objects:
            yield from obj.iter_dag_dependencies(source=source, target=target)


class AssetAny(AssetBooleanCondition):
    """Use to combine assets schedule references in an "or" relationship."""

    agg_func = any  # type: ignore[assignment]

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


class AssetAll(AssetBooleanCondition):
    """Use to combine assets schedule references in an "and" relationship."""

    agg_func = all  # type: ignore[assignment]

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


@attrs.define
class AssetAliasEvent(attrs.AttrsInstance):
    """Representation of asset event to be triggered by an asset alias."""

    source_alias_name: str
    dest_asset_key: AssetUniqueKey
    dest_asset_extra: dict[str, JsonValue]
    extra: dict[str, JsonValue]
