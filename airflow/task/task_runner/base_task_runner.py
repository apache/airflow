# -*- coding: utf-8 -*-
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
from __future__ import unicode_literals

import getpass
import os
import subprocess
import threading

from airflow.utils.log.logging_mixin import LoggingMixin

from airflow import configuration as conf
from airflow.utils.configuration import tmp_configuration_copy


PYTHONPATH_VAR = 'PYTHONPATH'


class BaseTaskRunner(LoggingMixin):
    """
    Runs Airflow task instances by invoking the `airflow run` command with raw
    mode enabled in a subprocess.
    """

    def __init__(self, local_task_job):
        """
        :param local_task_job: The local task job associated with running the
        associated task instance.
        :type local_task_job: airflow.jobs.LocalTaskJob
        """
        # Pass task instance context into log handlers to setup the logger.
        super(BaseTaskRunner, self).__init__(local_task_job.task_instance)
        self._task_instance = local_task_job.task_instance

        popen_prepend = []
        if self._task_instance.run_as_user:
            self.run_as_user = self._task_instance.run_as_user
        else:
            try:
                self.run_as_user = conf.get('core', 'default_impersonation')
            except conf.AirflowConfigException:
                self.run_as_user = None

        # Always provide a copy of the configuration file settings
        cfg_path = tmp_configuration_copy()
        # The following command should always work since the user doing chmod is the same
        # as the one who just created the file.
        subprocess.call(
            ['chmod', '600', cfg_path],
            close_fds=True
        )

        # Add sudo commands to change user if we need to. Needed to handle SubDagOperator
        # case using a SequentialExecutor.
        self.log.debug("Planning to run as the %s user", self.run_as_user)
        if self.run_as_user and (self.run_as_user != getpass.getuser()):
            # Give ownership of file to user; only they can read and write
            subprocess.call(
                ['sudo', 'chown', self.run_as_user, cfg_path],
                close_fds=True
            )

            # propagate PYTHONPATH environment variable
            pythonpath_value = os.environ.get(PYTHONPATH_VAR, '')
            popen_prepend = ['sudo', '-E', '-H', '-u', self.run_as_user]

            if pythonpath_value:
                popen_prepend.append('{}={}'.format(PYTHONPATH_VAR, pythonpath_value))

        self._cfg_path = cfg_path
        self._command = popen_prepend + self._task_instance.command_as_list(
            raw=True,
            pickle_id=local_task_job.pickle_id,
            mark_success=local_task_job.mark_success,
            job_id=local_task_job.id,
            pool=local_task_job.pool,
            cfg_path=cfg_path,
        )
        self.process = None

    def _read_task_logs(self, stream):
        while True:
            line = stream.readline()
            if isinstance(line, bytes):
                line = line.decode('utf-8')
            if len(line) == 0:
                break
            self.log.info('Job %s: Subtask %s %s',
                          self._task_instance.job_id, self._task_instance.task_id,
                          line.rstrip('\n'))

    def run_command(self, run_with, join_args=False):
        """
        Run the task command

        :param run_with: list of tokens to run the task command with
        E.g. ['bash', '-c']
        :type run_with: list
        :param join_args: whether to concatenate the list of command tokens
        E.g. ['airflow', 'run'] vs ['airflow run']
        :param join_args: bool
        :return: the process that was run
        :rtype: subprocess.Popen
        """
        cmd = [" ".join(self._command)] if join_args else self._command
        full_cmd = run_with + cmd
        self.log.info('Running: %s', full_cmd)
        proc = subprocess.Popen(
            full_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
            close_fds=True,
            env=os.environ.copy(),
            preexec_fn=os.setsid
        )

        # Start daemon thread to read subprocess logging output
        log_reader = threading.Thread(
            target=self._read_task_logs,
            args=(proc.stdout,),
        )
        log_reader.daemon = True
        log_reader.start()
        return proc

    def start(self):
        """
        Start running the task instance in a subprocess.
        """
        raise NotImplementedError()

    def return_code(self):
        """
        :return: The return code associated with running the task instance or
        None if the task is not yet done.
        :rtype int:
        """
        raise NotImplementedError()

    def terminate(self):
        """
        Kill the running task instance.
        """
        raise NotImplementedError()

    def on_finish(self):
        """
        A callback that should be called when this is done running.
        """
        if self._cfg_path and os.path.isfile(self._cfg_path):
            subprocess.call(['sudo', 'rm', self._cfg_path], close_fds=True)
