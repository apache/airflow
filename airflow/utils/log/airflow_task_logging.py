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
#
import logging
import os
import warnings

from airflow import configuration as conf
from airflow.configuration import AirflowConfigException
from airflow.utils import logging as logging_utils
from airflow.utils.file import mkdirs
from airflow.utils.log.base_airflow_task_logging import BaseAirflowTaskLogging
from airflow.utils.state import State


class AirflowTaskLogging(BaseAirflowTaskLogging):
    """
    Default configuration for airflow logging behaviors. It logs into a local
    file named `dag_id/task_id/execution_date` and optionally uploads to S3/GCS
    on task completion.
    """

    def __init__(self, name='default'):
        super(AirflowTaskLogging, self).__init__(name)

    def setup_task_logging(self, task_instance):
        logging.root.handlers = []
        # Setting up logging to a file.

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
        log_base = os.path.expanduser(conf.get('core', 'BASE_LOG_FOLDER'))
        directory = log_base + "/{task_instance.dag_id}/{task_instance.task_id}" \
            .format(task_instance=task_instance)
        # Create the log file and give it group writable permissions
        # TODO(aoen): Make log dirs and logs globally readable for now since the SubDag
        # operator is not compatible with impersonation (e.g. if a Celery executor is used
        # for a SubDag operator and the SubDag operator has a different owner than the
        # parent DAG)
        if not os.path.exists(directory):
            # Create the directory as globally writable using custom mkdirs
            # as os.makedirs doesn't set mode properly.
            mkdirs(directory, 0o775)
        iso = task_instance.execution_date.isoformat()
        filename = "{directory}/{iso}".format(directory=directory, iso=iso)

        if not os.path.exists(filename):
            open(filename, "a").close()
            os.chmod(filename, 0o666)

        logging_level = conf.get('core', 'LOGGING_LEVEL').upper()
        log_format = conf.get('core', 'log_format')

        logging.basicConfig(
            filename=filename,
            level=logging_level,
            format=log_format)

    def get_task_logger(self, task_instance):
        return logging.root

    def post_task_logging(self, task_instance):
        # Force the log to flush, and set the handler to go back to normal so we
        # don't continue logging to the task's log file. The flush is important
        # because we subsequently read from the log to insert into S3 or Google
        # cloud storage.
        logging.root.handlers[0].flush()
        logging.root.handlers = []

        # store logs remotely
        remote_base = conf.get('core', 'REMOTE_BASE_LOG_FOLDER')

        # deprecated as of March 2016
        if not remote_base and conf.get('core', 'S3_LOG_FOLDER'):
            warnings.warn(
                'The S3_LOG_FOLDER conf key has been replaced by '
                'REMOTE_BASE_LOG_FOLDER. Your conf still works but please '
                'update airflow.cfg to ensure future compatibility.',
                DeprecationWarning)
            remote_base = conf.get('core', 'S3_LOG_FOLDER')

        log_base = os.path.expanduser(conf.get('core', 'BASE_LOG_FOLDER'))
        relative_path = self._get_log_filename(task_instance)
        filename = os.path.join(log_base, relative_path)

        if os.path.exists(filename):
            # read log and remove old logs to get just the latest additions

            with open(filename, 'r') as logfile:
                log = logfile.read()

            remote_log_location = filename.replace(log_base, remote_base)
            # S3
            if remote_base.startswith('s3:/'):
                logging_utils.S3Log().write(log, remote_log_location)
            # GCS
            elif remote_base.startswith('gs:/'):
                logging_utils.GCSLog().write(log, remote_log_location)
            # Other
            elif remote_base and remote_base != 'None':
                logging.error(
                    'Unsupported remote log location: {}'.format(remote_base))

    def get_task_logs(self, task_instance):
        base_log_folder = os.path.expanduser(conf.get('core', 'BASE_LOG_FOLDER'))
        log_relative = self._get_log_filename(task_instance)
        loc = os.path.join(base_log_folder, log_relative)
        log = ""

        # load remote logs
        remote_log_base = conf.get('core', 'REMOTE_BASE_LOG_FOLDER')
        remote_log_loaded = False
        if remote_log_base:
            remote_log_path = os.path.join(remote_log_base, log_relative)
            remote_log = ""

            # Only display errors reading the log if the task completed or ran at least
            # once before (otherwise there won't be any remote log stored).
            ti_execution_completed = task_instance.state in {State.SUCCESS, State.FAILED}
            ti_ran_more_than_once = task_instance.try_number > 1
            surface_log_retrieval_errors = (
                ti_execution_completed or ti_ran_more_than_once)

            # S3
            if remote_log_path.startswith('s3:/'):
                remote_log += logging_utils.S3Log().read(
                    remote_log_path, return_error=surface_log_retrieval_errors)
                remote_log_loaded = True
            # GCS
            elif remote_log_path.startswith('gs:/'):
                remote_log += logging_utils.GCSLog().read(
                    remote_log_path, return_error=surface_log_retrieval_errors)
                remote_log_loaded = True
            # unsupported
            else:
                remote_log += '*** Unsupported remote log location.'

            if remote_log:
                log += ('*** Reading remote log from {}.\n{}\n'.format(
                    remote_log_path, remote_log))

        # We only want to display the
        # local logs while the task is running if a remote log configuration is set up
        # since the logs will be transfered there after the run completes.
        # TODO(aoen): One problem here is that if a task is running on a worker it
        # already ran on, then duplicate logs will be printed for all of the previous
        # runs of the task that already completed since they will have been printed as
        # part of the remote log section above. This can be fixed either by streaming
        # logs to the log servers as tasks are running, or by creating a proper
        # abstraction for multiple task instance runs).
        if not remote_log_loaded or task_instance.state == State.RUNNING:
            if os.path.exists(loc):
                try:
                    with open(loc) as f:
                        log += "*** Reading local log.\n" + "".join(f.readlines())
                except Exception:
                    log = "*** Failed to load local log file: {0}.\n".format(loc)
            else:
                url = os.path.join(
                    "http://{ti.hostname}:{worker_log_server_port}/log", log_relative
                ).format(ti=task_instance,
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

    def _get_log_filename(self, task_instance):
        return "{dag_id}/{task_id}/{execution_date}".format(
            dag_id=task_instance.dag_id, task_id=task_instance.task_id,
            execution_date=task_instance.execution_date.isoformat())
