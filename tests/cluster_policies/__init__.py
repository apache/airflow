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
from typing import Callable, List

from airflow.configuration import conf
from airflow.exceptions import AirflowClusterPolicyViolation
from airflow.models.baseoperator import BaseOperator


# [START example_cluster_policy_rule]
def task_must_have_owners(task):
    if not task.owner or task.owner.lower() == conf.get('operators', 'default_owner'):
        raise AirflowClusterPolicyViolation(
            'Task must have non-None non-default owner. Current value: {}'.format(task.owner))
# [END example_cluster_policy_rule]


# [START example_list_of_cluster_policy_rules]
TASK_RULES = [task_must_have_owners]   # type: List[Callable[[BaseOperator], None]]


def _check_task_rules(current_task):
    """Check task rules for given task."""
    notices = []
    for rule in TASK_RULES:
        try:
            rule(current_task)
        except AirflowClusterPolicyViolation as ex:
            notices.append(str(ex))
    if notices:
        notices_list = " * " + "\n * ".join(notices)
        raise AirflowClusterPolicyViolation(
            "DAG policy violation (DAG ID: {0}, Path: {1}):\n"
            "Notices:\n"
            "{2}".format(current_task.dag_id, current_task.dag.filepath, notices_list))


def cluster_policy(task):
    """Ensure Tasks have non-default owners."""
    _check_task_rules(task)
# [END example_list_of_cluster_policy_rules]
