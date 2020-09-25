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

# TODO(mik-laj): We have to implement it.
#     Do you want to help? Please look at: https://github.com/apache/airflow/issues/8132
from airflow.api_connexion import security


@security.requires_access([("can_read", "Dag"), ("can_read", "DagRun"), ("can_read", "Task")])
def get_task_instance():
    """
    Get a task instance
    """
    raise NotImplementedError("Not implemented yet.")


@security.requires_access([("can_read", "Dag"), ("can_read", "DagRun"), ("can_read", "Task")])
def get_task_instances():
    """
    Get list of task instances of DAG.
    """
    raise NotImplementedError("Not implemented yet.")


@security.requires_access(
    [
        ('can_read', 'Dag'),
        ('can_read', 'DagRun'),
        ('can_read', 'Task'),
        ('can_read', 'TaskInstance'),
    ]
)
def get_task_instances_batch():
    """
    Get list of task instances.
    """
    raise NotImplementedError("Not implemented yet.")


@security.requires_access([("can_read", "Dag"), ("can_read", "DagRun"), ("can_edit", "Task")])
def post_clear_task_instances():
    """
    Clear task instances.
    """
    raise NotImplementedError("Not implemented yet.")
