
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
from airflow.utils import timezone


class QueueTaskRun:  # pylint: disable=too-many-instance-attributes
    """
    Generates the shell command required to execute this task instance.

    :param dag_id: DAG ID
    :type dag_id: unicode
    :param task_id: Task ID
    :type task_id: unicode
    :param execution_date: Execution date for the task
    :type execution_date: datetime.datetime
    :param mark_success: Whether to mark the task as successful
    :type mark_success: bool
    :param pickle_id: If the DAG was serialized to the DB, the ID
        associated with the pickled DAG
    :type pickle_id: unicode
    :param subdir: path to the file containing the DAG definition
    :param raw: raw mode (needs more details)
    :param job_id: job ID (needs more details)
    :param pool: the Airflow pool that the task should run in
    :type pool: unicode
    :param cfg_path: the Path to the configuration file
    :type cfg_path: str
    :return: shell command that can be used to run the task instance
    """
    def __init__(  # pylint: disable=too-many-arguments
        self,
        dag_id,
        task_id,
        execution_date,
        mark_success=None,
        pickle_id=None,
        job_id=None,
        force=None,
        pool=None,
        raw=None,
        subdir=None,
        cfg_path=None,
        mock_command=None,
    ):
        self.dag_id = dag_id
        self.task_id = task_id
        if isinstance(execution_date, str):
            self.execution_date = timezone.parse(execution_date)
        else:
            self.execution_date = execution_date
        self.mark_success = mark_success
        self.pickle_id = pickle_id
        self.job_id = job_id
        self.force = force
        self.pool = pool
        self.raw = raw
        self.subdir = subdir
        self.cfg_path = cfg_path
        self.mock_command = mock_command

    def as_command(self):
        """Generate CLI command"""
        if self.mock_command:
            return self.mock_command
        iso = self.execution_date.isoformat()
        cmd = ["airflow", "tasks", "run", str(self.dag_id), str(self.task_id), str(iso)]
        if self.mark_success:
            cmd.extend(["--mark_success"])
        if self.pickle_id:
            cmd.extend(["--pickle", str(self.pickle_id)])
        if self.job_id:
            cmd.extend(["--job_id", str(self.job_id)])
        if self.force:
            cmd.extend(["--force"])
        if self.pool:
            cmd.extend(["--pool", self.pool])
        if self.raw:
            cmd.extend(["--raw"])
        if self.subdir:
            cmd.extend(["--subdir", self.subdir])
        if self.cfg_path:
            cmd.extend(["--cfg_path", self.cfg_path])
        return cmd

    def __repr__(self):
        iso = self.execution_date.isoformat()
        return f"QueueTaskRun(dag_id={self.dag_id}, task_id={self.task_id}, execution_date={iso})"


class LocalTaskJobDeferredRun:  # pylint: disable=too-many-instance-attributes
    """
    Generates the shell command required to execute this task instance.

    :param dag_id: DAG ID
    :type dag_id: unicode
    :param task_id: Task ID
    :type task_id: unicode
    :param execution_date: Execution date for the task
    :type execution_date: datetime.datetime
    :param mark_success: Whether to mark the task as successful
    :type mark_success: bool
    :param ignore_all_dependencies: Ignore all ignorable dependencies.
        Overrides the other ignore_* parameters.
    :type ignore_all_dependencies: bool
    :param ignore_depends_on_past: Ignore depends_on_past parameter of DAGs
        (e.g. for Backfills)
    :type ignore_depends_on_past: bool
    :param ignore_dependencies: Ignore task-specific dependencies such as depends_on_past
        and trigger rule
    :type ignore_dependencies: bool
    :param force: Ignore the task instance's previous failure/success
    :type force: bool
    :param local: Whether to run the task locally
    :type local: bool
    :param pickle_id: If the DAG was serialized to the DB, the ID
        associated with the pickled DAG
    :type pickle_id: unicode
    :param subdir: path to the file containing the DAG definition
    :param raw: raw mode (needs more details)
    :param job_id: job ID (needs more details)
    :param pool: the Airflow pool that the task should run in
    :type pool: unicode
    :param cfg_path: the Path to the configuration file
    :type cfg_path: str
    :return: shell command that can be used to run the task instance
    """
    def __init__(  # pylint: disable=too-many-arguments
        self,
        dag_id,
        task_id,
        execution_date,
        mark_success=None,
        pickle_id=None,
        job_id=None,
        ignore_all_dependencies=None,
        ignore_dependencies=None,
        ignore_depends_on_past=None,
        force=None,
        local=None,
        pool=None,
        raw=None,
        subdir=None,
        cfg_path=None,
        mock_command=None,
    ):
        self.dag_id = dag_id
        self.task_id = task_id
        if isinstance(execution_date, str):
            self.execution_date = timezone.parse(execution_date)
        else:
            self.execution_date = execution_date
        self.mark_success = mark_success
        self.pickle_id = pickle_id
        self.job_id = job_id
        self.ignore_all_dependencies = ignore_all_dependencies
        self.ignore_dependencies = ignore_dependencies
        self.ignore_depends_on_past = ignore_depends_on_past
        self.force = force
        self.local = local
        self.pool = pool
        self.raw = raw
        self.subdir = subdir
        self.cfg_path = cfg_path
        self.mock_command = mock_command

    def as_command(self):
        """Generate CLI command"""
        if self.mock_command:
            return self.mock_command
        iso = self.execution_date.isoformat()
        cmd = ["airflow", "tasks", "run", str(self.dag_id), str(self.task_id), str(iso), "--local"]
        if self.mark_success:
            cmd.extend(["--mark_success"])
        if self.pickle_id:
            cmd.extend(["--pickle", str(self.pickle_id)])
        if self.job_id:
            cmd.extend(["--job_id", str(self.job_id)])
        if self.ignore_all_dependencies:
            cmd.extend(["--ignore_all_dependencies"])
        if self.ignore_dependencies:
            cmd.extend(["--ignore_dependencies"])
        if self.ignore_depends_on_past:
            cmd.extend(["--ignore_depends_on_past"])
        if self.force:
            cmd.extend(["--force"])
        if self.pool:
            cmd.extend(["--pool", self.pool])
        if self.subdir:
            cmd.extend(["--subdir", self.subdir])
        if self.cfg_path:
            cmd.extend(["--cfg_path", self.cfg_path])
        return cmd

    def __repr__(self):
        iso = self.execution_date.isoformat()
        return f"LocalTaskJobDeferredRun(dag_id={self.dag_id}, task_id={self.task_id}, execution_date={iso})"
