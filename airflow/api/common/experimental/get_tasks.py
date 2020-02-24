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
"""Tasks APIs.."""
import re
import typing

from airflow.api.common.experimental import check_and_get_dag


def get_tasks(dag_id: str, regex: str = '') -> typing.List[str]:
    """
    Return the list of tasks of a given dag identified by the given dag_id.
    Optionally filter the task list with a `regex` string.
    """
    dag = check_and_get_dag(dag_id)

    if regex:
        pattern = re.compile(regex)
        return list(filter(pattern.match, dag.task_ids))
    else:
        return dag.task_ids
