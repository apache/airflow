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

import typing

if typing.TYPE_CHECKING:
    from airflow.models import TaskInstance
    from airflow.providers.openlineage.extractors import OperatorLineage


class OpenLineageMixin:
    """
    This interface marks implementation of OpenLineage methods,
    allowing us to check for its existence rather than existence of particular methods on BaseOperator.
    """

    def get_openlineage_facets_on_start(self) -> OperatorLineage | None:
        raise NotImplementedError()

    def get_openlineage_facets_on_complete(self, task_instance: TaskInstance) -> OperatorLineage | None:
        return self.get_openlineage_facets_on_start()

    def get_openlineage_facets_on_fail(self, task_instance: TaskInstance) -> OperatorLineage | None:
        return self.get_openlineage_facets_on_complete(task_instance)
