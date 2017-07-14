# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import os

from airflow import configuration as conf
from airflow.configuration import AirflowConfigException
from airflow.utils.file import mkdirs


class FileTaskHandler(logging.Handler):
    """
    FileTaskHandler is a python log handler that handles and reads
    task instance logs. It creates and delegates log handling
    to `logging.FileHandler` after receiving task instance context.
    It reads logs from task instance's host machine.
    """

    def __init__(self, base_log_folder):
        super(FileTaskHandler, self).__init__()
        self.handler = None
        self.local_base = os.path.expanduser(base_log_folder)

    def set_context(self, task_instance):
        """
        Provide task_instance context to airflow task handler.
        :param task_instance: task instance object
        """
        self._init_file(task_instance)
        local_loc = self.get_local_loc(task_instance)
        self.handler = logging.FileHandler(local_loc)
        self.handler.setFormatter(self.formatter)
        self.handler.setLevel(self.level)

    def emit(self, record):
        if self.handler:
            self.handler.emit(record)

    def flush(self):
        if self.handler:
            self.handler.flush()

    def close(self):
        if self.handler:
            self.handler.close()

    def read(self, task_instance):
        """
        Read log of given task instance from task instance host server.
        :param task_instance: task instance database record
        """
        log = ""
        # Task instance here might be different from task instance when
        # initializing the handler. Thus explicitly getting log location
        # is needed to get correct log path.
        loc = self.get_local_loc(task_instance)

        if os.path.exists(loc):
            try:
                with open(loc) as f:
                    log += "*** Reading local log.\n" + "".join(f.readlines())
            except Exception:
                log = "*** Failed to load local log file: {0}.\n".format(loc)
        else:
            url = os.path.join("http://{ti.hostname}:{worker_log_server_port}/log",
                               self.get_log_relative_path(task_instance)).format(
                ti=task_instance,
                worker_log_server_port=conf.get('celery', 'WORKER_LOG_SERVER_PORT'))
            log += "*** Log file isn't local.\n"
            log += "*** Fetching here: {url}\n".format(**locals())
            try:
                import requests
                timeout = None  # No timeout
                try:
                    timeout = conf.getint('webserver', 'log_fetch_timeout_sec')
                except (AirflowConfigException, ValueError):
                    pass

                response = requests.get(url, timeout=timeout)
                response.raise_for_status()
                log += '\n' + response.text
            except Exception:
                log += "*** Failed to fetch log file from worker.\n".format(
                    **locals())
        return log

    def get_log_relative_dir(self, ti):
        """
        Get relative log file directory.
        :param ti: task instance object
        """
        return "{}/{}".format(ti.dag_id, ti.task_id)

    def get_log_relative_path(self, ti):
        """
        Get relative log file path.
        :param ti: task instance object
        """
        directory = self.get_log_relative_dir(ti)
        filename = "{}.log".format(ti.execution_date.isoformat())
        return os.path.join(directory, filename)

    def get_local_loc(self, ti):
        """
        Get full local log path given task instance object.
        :param ti: task instance object
        """
        log_relative_path = self.get_log_relative_path(ti)
        return os.path.join(self.local_base, log_relative_path)

    def _init_file(self, ti):
        """
        Create log directory and give it correct permissions.
        :param ti: task instance object
        :return relative log path of the given task instance
        """
        # To handle log writing when tasks are impersonated, the log files need to
        # be writable by the user that runs the Airflow command and the user
        # that is impersonated. This is mainly to handle corner cases with the
        # SubDagOperator. When the SubDagOperator is run, all of the operators
        # run under the impersonated user and create appropriate log files
        # as the impersonated user. However, if the user manually runs tasks
        # of the SubDagOperator through the UI, then the log files are created
        # by the user that runs the Airflow command. For example, the Airflow
        # run command may be run by the `airflow_sudoable` user, but the Airflow
        # tasks may be run by the `airflow` user. If the log files are not
        # writable by both users, then it's possible that re-running a task
        # via the UI (or vice versa) results in a permission error as the task
        # tries to write to a log file created by the other user.
        relative_log_dir = self.get_log_relative_dir(ti)
        directory = os.path.join(self.local_base, relative_log_dir)
        # Create the log file and give it group writable permissions
        # TODO(aoen): Make log dirs and logs globally readable for now since the SubDag
        # operator is not compatible with impersonation (e.g. if a Celery executor is used
        # for a SubDag operator and the SubDag operator has a different owner than the
        # parent DAG)
        if not os.path.exists(directory):
            # Create the directory as globally writable using custom mkdirs
            # as os.makedirs doesn't set mode properly.
            mkdirs(directory, 0o775)

        local_loc = self.get_local_loc(ti)

        if not os.path.exists(local_loc):
            open(local_loc, "a").close()
            os.chmod(local_loc, 0o666)
