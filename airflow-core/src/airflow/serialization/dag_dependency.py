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

from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True, order=True)
class DagDependency:
    """
    Dataclass for representing dependencies between dags.

    These are calculated during serialization and attached to serialized dags.

    The source and target keys store the information of what component depends on what.
    For an asset related dependency, a root node will have the source value equal to its dependency_type and
    an end node will have the target value equal to its dependency_type. It's easier to explain by examples.

    For the example below,

    .. code-block:: python

        # we assume the asset is active
        DAG(dag_id="dag_1", schedule=[Asset.ref(uri="uri")])

    we get dag dependency like

    .. code-block:: python

        DagDependency(
            source="asset",
            target="dag_1",
            label="name",  # asset name, we always use asset name as label
            dependency_type="asset",
            dependency_id=1,  # asset id
        )

    This will look like `Asset name` -> `Dag dag_1` on the dependency graph. This is a root asset node as it
    has the source value as asset, and it points to its target "dag_1"

    For more complex dependency like asset alias,

    .. code-block:: python

        # we assume the asset is active
        DAG(
            dag_id="dag_2",
            schedule=[
                AssetAlias(name="alias_1"),  # resolved into Asset(uri="uri", name="name")
                AssetAlias(name="alias_2"),  # resolved to nothing
            ],
        )

    we'll need to store more data,

    .. code-block:: python

        [
            DagDependency(
                source="asset",
                target="asset-alias:alias_1",
                label="name",
                dependency_type="asset",
                dependency_id="1",
            ),
            DagDependency(
                source="asset:1",
                target="dag_2",
                label="alias_1",
                dependency_type="asset-alias",
                dependency_id="alias_1",
            ),
            DagDependency(
                source="asset-alias",
                target="dag_2",
                label="alias_2",
                dependency_type="asset-alias",
                dependency_id="alias_2",
            ),
        ]


    We want it to look like `Asset name` -> `AssetAlias alias_1` -> `Dag dag_1` on the dependency graph. The
    first node here is a root node point to an asset alias. Thus, its target is set to the asset we're point
    to. The second node represents the asset alias points to this asset and then this asset points to the dag.
    The third node represents a dependency between an asset alias and dag directly as it's not resolved.

    For asset ref cases, it works similar to asset if it's a valid asset ref. If not, it works the same as
    an unresolved asset alias.
    """

    source: str
    target: str
    label: str
    dependency_type: Literal["asset", "asset-alias", "asset-name-ref", "asset-uri-ref", "trigger", "sensor"]
    dependency_id: str | None = None

    @property
    def node_id(self):
        """Node ID for graph rendering."""
        val = f"{self.dependency_type}"
        if self.dependency_type not in ("asset", "asset-alias", "asset-name-ref", "asset-uri-ref"):
            val = f"{val}:{self.source}:{self.target}"
        if self.dependency_id:
            val = f"{val}:{self.dependency_id}"
        return val
