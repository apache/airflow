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

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, ClassVar, Literal

import attrs

from airflow.api_fastapi.execution_api.datamodels.asset import AssetProfile
from airflow.serialization.dag_dependency import DagDependency

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Iterator, MutableSequence

    from typing_extensions import Self

    from airflow.models.asset import AssetModel

    AttrsInstance = attrs.AttrsInstance
else:
    AttrsInstance = object


@attrs.define(frozen=True)
class SerializedAssetUniqueKey(AttrsInstance):
    """
    Columns to identify an unique asset.

    :meta private:
    """

    name: str
    uri: str

    @classmethod
    def from_asset(cls, asset: SerializedAsset | AssetModel) -> Self:
        return cls(name=asset.name, uri=asset.uri)

    @classmethod
    def from_str(cls, key: str) -> Self:
        return cls(**json.loads(key))

    def to_str(self) -> str:
        return json.dumps(attrs.asdict(self))

    def asprofile(self) -> AssetProfile:
        return AssetProfile(name=self.name, uri=self.uri, type="Asset")


class SerializedAssetBase:
    """
    Protocol for all serialized asset-like objects.

    :meta private:
    """

    def __bool__(self) -> bool:
        return True

    def as_expression(self) -> Any:
        """
        Serialize the asset into its scheduling expression.

        The return value is stored in DagModel for display purposes. It must be
        JSON-compatible.

        :meta private:
        """
        raise NotImplementedError

    def iter_assets(self) -> Iterator[tuple[SerializedAssetUniqueKey, SerializedAsset]]:
        raise NotImplementedError

    def iter_asset_aliases(self) -> Iterator[tuple[str, SerializedAssetAlias]]:
        raise NotImplementedError

    def iter_asset_refs(self) -> Iterator[SerializedAssetRef]:
        raise NotImplementedError

    def iter_dag_dependencies(self, *, source: str, target: str) -> Iterator[DagDependency]:
        """
        Iterate a base asset as dag dependency.

        :meta private:
        """
        raise NotImplementedError


@attrs.define
class SerializedAssetWatcher:
    """Serialized representation of an asset watcher."""

    name: str
    trigger: dict


@attrs.define
class SerializedAsset(SerializedAssetBase):
    """Serialized representation of an asset."""

    name: str
    uri: str
    group: str
    extra: dict[str, Any]
    watchers: MutableSequence[SerializedAssetWatcher]

    def as_expression(self) -> Any:
        """
        Serialize the asset into its scheduling expression.

        :meta private:
        """
        return {"asset": {"uri": self.uri, "name": self.name, "group": self.group}}

    def iter_assets(self) -> Iterator[tuple[SerializedAssetUniqueKey, SerializedAsset]]:
        yield SerializedAssetUniqueKey.from_asset(self), self

    def iter_asset_aliases(self) -> Iterator[tuple[str, SerializedAssetAlias]]:
        return iter(())

    def iter_asset_refs(self) -> Iterator[SerializedAssetRef]:
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
            dependency_id=SerializedAssetUniqueKey.from_asset(self).to_str(),
        )

    def asprofile(self) -> AssetProfile:
        """
        Profiles Asset to AssetProfile.

        :meta private:
        """
        return AssetProfile(name=self.name or None, uri=self.uri or None, type="Asset")


class SerializedAssetRef(SerializedAssetBase, AttrsInstance):
    """Serialized representation of an asset reference."""

    _dependency_type: Literal["asset-name-ref", "asset-uri-ref"]

    def as_expression(self) -> Any:
        return {"asset_ref": attrs.asdict(self)}

    def iter_assets(self) -> Iterator[tuple[SerializedAssetUniqueKey, SerializedAsset]]:
        return iter(())

    def iter_asset_aliases(self) -> Iterator[tuple[str, SerializedAssetAlias]]:
        return iter(())

    def iter_asset_refs(self) -> Iterator[SerializedAssetRef]:
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
class SerializedAssetNameRef(SerializedAssetRef):
    """Serialized representation of an asset reference by name."""

    name: str

    _dependency_type = "asset-name-ref"


@attrs.define(hash=True)
class SerializedAssetUriRef(SerializedAssetRef):
    """Serialized representation of an asset reference by URI."""

    uri: str

    _dependency_type = "asset-uri-ref"


@attrs.define
class SerializedAssetAlias(SerializedAssetBase):
    """Serialized representation of an asset alias."""

    name: str
    group: str

    def as_expression(self) -> Any:
        """
        Serialize the asset alias into its scheduling expression.

        :meta private:
        """
        return {"alias": {"name": self.name, "group": self.group}}

    def iter_assets(self) -> Iterator[tuple[SerializedAssetUniqueKey, SerializedAsset]]:
        return iter(())

    def iter_asset_aliases(self) -> Iterator[tuple[str, SerializedAssetAlias]]:
        yield self.name, self

    def iter_asset_refs(self) -> Iterator[SerializedAssetRef]:
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


@attrs.define
class SerializedAssetBooleanCondition(SerializedAssetBase):
    """Serialized representation of an asset condition."""

    objects: list[SerializedAssetBase]

    agg_func: ClassVar[Callable[[Iterable], bool]]

    def iter_assets(self) -> Iterator[tuple[SerializedAssetUniqueKey, SerializedAsset]]:
        for o in self.objects:
            yield from o.iter_assets()

    def iter_asset_aliases(self) -> Iterator[tuple[str, SerializedAssetAlias]]:
        for o in self.objects:
            yield from o.iter_asset_aliases()

    def iter_asset_refs(self) -> Iterator[SerializedAssetRef]:
        for o in self.objects:
            yield from o.iter_asset_refs()

    def iter_dag_dependencies(self, *, source: str, target: str) -> Iterator[DagDependency]:
        """
        Iterate asset, asset aliases and their resolved assets  as dag dependency.

        :meta private:
        """
        for obj in self.objects:
            yield from obj.iter_dag_dependencies(source=source, target=target)


class SerializedAssetAny(SerializedAssetBooleanCondition):
    """Serialized representation of an asset "or" relationship."""

    agg_func = any

    def as_expression(self) -> dict[str, Any]:
        """
        Serialize the asset into its scheduling expression.

        :meta private:
        """
        return {"any": [o.as_expression() for o in self.objects]}


class SerializedAssetAll(SerializedAssetBooleanCondition):
    """Serialized representation of an asset "and" relationship."""

    agg_func = all

    def __repr__(self) -> str:
        return f"AssetAny({', '.join(map(str, self.objects))})"

    def as_expression(self) -> Any:
        """
        Serialize the assets into its scheduling expression.

        :meta private:
        """
        return {"all": [o.as_expression() for o in self.objects]}
