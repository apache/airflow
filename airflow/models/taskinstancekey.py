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

from typing import NamedTuple


class TaskInstanceKey(NamedTuple):
    """Key used to identify task instance."""

    dag_id: str
    task_id: str
    run_id: str
    try_number: int = 1
    map_index: int = -1

    @property
    def primary(self) -> tuple[str, str, str, int]:
        """Return task instance primary key part of the key."""
        return self.dag_id, self.task_id, self.run_id, self.map_index

    def with_try_number(self, try_number: int) -> TaskInstanceKey:
        """Return TaskInstanceKey with provided ``try_number``."""
        return TaskInstanceKey(self.dag_id, self.task_id, self.run_id, try_number, self.map_index)

    @property
    def key(self) -> TaskInstanceKey:
        """
        For API-compatibly with TaskInstance.

        Returns self
        """
        return self

    @classmethod
    def from_dict(cls, dictionary):
        """Create TaskInstanceKey from dictionary."""
        return cls(**dictionary)
