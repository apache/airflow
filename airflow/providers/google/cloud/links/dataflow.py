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
"""This module contains Google Dataflow links."""
from datetime import datetime
from typing import TYPE_CHECKING, Optional

from airflow.models import BaseOperator, BaseOperatorLink, XCom

if TYPE_CHECKING:
    from airflow.utils.context import Context

DATAFLOW_BASE_LINK = "https://pantheon.corp.google.com/dataflow/jobs"
DATAFLOW_JOB_LINK = DATAFLOW_BASE_LINK + "/{region}/{job_id}?project={project_id}"


class DataflowJobLink(BaseOperatorLink):
    """Helper class for constructing Dataflow Job Link"""

    name = "Dataflow Job"
    key = "dataflow_job_config"

    @staticmethod
    def persist(
        operator_instance: BaseOperator,
        context: "Context",
        project_id: Optional[str],
        region: Optional[str],
        job_id: Optional[str],
    ):
        operator_instance.xcom_push(
            context,
            key=DataflowJobLink.key,
            value={"project_id": project_id, "location": region, "job_id": job_id},
        )

    def get_link(self, operator: BaseOperator, dttm: datetime) -> str:
        conf = XCom.get_one(
            key=DataflowJobLink.key,
            dag_id=operator.dag.dag_id,
            task_id=operator.task_id,
            execution_date=dttm,
        )
        return (
            DATAFLOW_JOB_LINK.format(
                project_id=conf["project_id"], region=conf['region'], job_id=conf['job_id']
            )
            if conf
            else ""
        )
