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
from openlineage.client.facet_v2 import JobFacet, RunFacet
from openlineage.client.utils import RedactMixin


@define
class AirflowMappedTaskRunFacet(RunFacet):
    """Run facet containing information about mapped tasks."""

    mapIndex: int
    operatorClass: str

    _additional_skip_redact = ["operatorClass"]

    @classmethod
    def from_task_instance(cls, task_instance):
        from airflow.providers.openlineage.utils.utils import get_fully_qualified_class_name

        return cls(
            mapIndex=task_instance.map_index,
            operatorClass=get_fully_qualified_class_name(task_instance.task),
        )


@define
class AirflowJobFacet(JobFacet):
    """
    Composite Airflow job facet.

    This facet encapsulates all the necessary information to re-create full scope of an Airflow DAG logic,
    enabling reconstruction, visualization, and analysis of DAGs in a comprehensive manner.
    It includes detailed representations of the tasks, task groups, and their hierarchical relationships,
    making it possible to draw a graph that visually represents the entire DAG structure (like in Airflow UI).
    It also indicates whether a task should emit an OpenLineage (OL) event, enabling consumers to anticipate
    the number of events and identify the tasks from which they can expect these events.

    Attributes:
        taskTree: A dictionary representing the hierarchical structure of tasks in the DAG.
        taskGroups: A dictionary that contains information about task groups within the DAG.
        tasks: A dictionary detailing individual tasks within the DAG.
    """

    taskTree: dict
    taskGroups: dict
    tasks: dict


@define
class AirflowStateRunFacet(RunFacet):
    """
    Airflow facet providing state information.

    This facet is designed to be sent at a completion event, offering state information about
    the DAG run and each individual task. This information is crucial for understanding
    the execution flow and comprehensive post-run analysis and debugging, including why certain tasks
    did not emit events, which can occur due to the use of control flow operators like the BranchOperator.

    Attributes:
        dagRunState: This indicates the final status of the entire DAG run (e.g., "success", "failed").
        tasksState: A dictionary mapping task IDs to their respective states. (e.g., "failed", "skipped").
    """

    dagRunState: str
    tasksState: dict[str, str]


@define
class AirflowRunFacet(RunFacet):
    """Composite Airflow run facet."""

    dag: dict
    dagRun: dict
    task: dict
    taskInstance: dict
    taskUuid: str


@define
class AirflowDagRunFacet(RunFacet):
    """Composite Airflow DAG run facet."""

    dag: dict
    dagRun: dict


@define
class AirflowDebugRunFacet(RunFacet):
    """Airflow Debug run facet."""

    packages: dict


@define
class UnknownOperatorInstance(RedactMixin):
    """
    Describes an unknown operator.

    This specifies the (class) name of the operator and its properties.
    """

    name: str
    properties: dict[str, object]
    type: str = "operator"

    _skip_redact = ["name", "type"]


@define
class UnknownOperatorAttributeRunFacet(RunFacet):
    """RunFacet that describes unknown operators in an Airflow DAG."""

    unknownItems: list[UnknownOperatorInstance]
