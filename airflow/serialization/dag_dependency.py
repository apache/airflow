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


@dataclass(frozen=True, order=True)
class DagDependency:
    """
    Dataclass for representing dependencies between DAGs.

    These are calculated during serialization and attached to serialized DAGs.
    """

    source: str
    target: str
    dependency_type: str
    dependency_id: str | None = None

    @property
    def node_id(self):
        """Node ID for graph rendering."""
        val = f"{self.dependency_type}"
        if self.dependency_type not in ("dataset", "dataset-alias"):
            val += f":{self.source}:{self.target}"
        if self.dependency_id:
            val += f":{self.dependency_id}"
        return val
