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

from attrs import define

from openlineage.client.facet import BaseFacet
from openlineage.client.utils import RedactMixin


@define(slots=False)
class AirflowMappedTaskRunFacet(BaseFacet):
    """Run facet containing information about mapped tasks"""

    mapIndex: int
    operatorClass: str

    _additional_skip_redact: list[str] = ["operatorClass"]

    @classmethod
    def from_task_instance(cls, task_instance):
        task = task_instance.task
        from airflow.providers.openlineage.utils import get_operator_class

        return cls(
            mapIndex=task_instance.map_index,
            operatorClass=f"{get_operator_class(task).__module__}.{get_operator_class(task).__name__}",
        )


@define(slots=False)
class AirflowRunFacet(BaseFacet):
    """Composite Airflow run facet."""

    dag: dict
    dagRun: dict
    task: dict
    taskInstance: dict
    taskUuid: str


@define(slots=False)
class UnknownOperatorInstance(RedactMixin):
    """
    Describes an unknown operator - specifies the (class) name of the operator
    and its properties
    """

    name: str
    properties: dict[str, object]
    type: str = "operator"

    _skip_redact: list[str] = ["name", "type"]


@define(slots=False)
class UnknownOperatorAttributeRunFacet(BaseFacet):
    """RunFacet that describes unknown operators in an Airflow DAG"""

    unknownItems: list[UnknownOperatorInstance]
