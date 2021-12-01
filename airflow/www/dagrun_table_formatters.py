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

from typing import Dict, Sequence

from markupsafe import Markup

from airflow.models import DAG, DagRun, TaskInstance, XCom
from airflow.www import utils as wwwutils

DagRunTableFormatRegistry = []


def get_formatter(dag: DAG):
    for f in DagRunTableFormatRegistry:
        if f.supports_dag(dag):
            return f
    return DefaultDagRunFormatter


def add_formatter_to_registry(f):
    DagRunTableFormatRegistry.append(f)


class DefaultDagRunFormatter:
    @staticmethod
    def labels(original_labels: Dict[str, str]):
        return {**original_labels, "conf": "Conf"}

    @staticmethod
    def extra_dagrun_fields(dr: DagRun, ti: Sequence[TaskInstance], session):
        return {"conf": dr.conf}

    @staticmethod
    def formatters():
        return {"conf": wwwutils.json_f('conf')}


class ExampleDagRunFormatter:
    """
    This formatter will provide two extra columns in the /dag_dagruns view,
      - One that extracts a value ('revision') from the conf that was used to start the dag
      - One that extracts a xcom return_value from a task in the dagrun
    """

    @staticmethod
    def supports_dag(dag: DAG):
        return dag.dag_id in ["example_xcom_args"]

    @staticmethod
    def labels(original_labels):
        return {
            **original_labels,
            "revision": "Revision",
            "return_value": "Generate Value Output",
            "logs": "Logs",
        }

    @staticmethod
    def extra_dagrun_fields(dr: DagRun, ti: Sequence[TaskInstance], session):
        wanted_tasks = [t for t in ti if t.task_id == "generate_value"]
        xcom_return_value = ""
        if len(wanted_tasks) > 0:
            wanted_task = wanted_tasks[0]
            try:
                xcom_return_value = (
                    session.query(XCom)
                    .filter(
                        XCom.dag_id == dr.dag_id,
                        XCom.task_id == wanted_task.task_id,
                        XCom.execution_date == wanted_task.execution_date,
                        XCom.key == "return_value",
                    )
                    .one()
                    .value
                )
            except Exception as e:
                xcom_return_value = str(e)

        return {"revision": dr.conf.get("revision"), "return_value": xcom_return_value}

    @staticmethod
    def formatters():
        def rev_formatter(attr):
            r = attr.get("revision") or ""
            return r[0:8]

        def log_formatter(attr):
            return Markup(
                '<a href="{url}"><span class="material-icons" aria-hidden="true">reorder</span></a>'
            ).format(url="http://example.com")

        return {"revision": rev_formatter, "logs": log_formatter}


add_formatter_to_registry(ExampleDagRunFormatter)
