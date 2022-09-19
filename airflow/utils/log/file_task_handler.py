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
"""File logging handler for tasks."""
from __future__ import annotations

import logging
import os
import warnings
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import urljoin

from airflow.configuration import AirflowConfigException, conf
from airflow.exceptions import RemovedInAirflow3Warning
from airflow.utils.context import Context
from airflow.utils.helpers import parse_template_string, render_template_to_string
from airflow.utils.log.non_caching_file_handler import NonCachingFileHandler
from airflow.utils.session import create_session
from airflow.utils.state import State

if TYPE_CHECKING:
    from airflow.models import TaskInstance


class FileTaskHandler(logging.Handler):
    """
    FileTaskHandler is a python log handler that handles and reads
    task instance logs. It creates and delegates log handling
    to `logging.FileHandler` after receiving task instance context.
    It reads logs from task instance's host machine.

    :param base_log_folder: Base log folder to place logs.
    :param filename_template: template filename string
    """

    def __init__(self, base_log_folder: str, filename_template: str | None = None):
        super().__init__()
        self.handler: logging.FileHandler | None = None
        self.local_base = base_log_folder
        if filename_template is not None:
            warnings.warn(
                "Passing filename_template to a log handler is deprecated and has no effect",
                RemovedInAirflow3Warning,
                # We want to reference the stack that actually instantiates the
                # handler, not the one that calls super()__init__.
                stacklevel=(2 if type(self) == FileTaskHandler else 3),
            )

    def set_context(self, ti: TaskInstance):
        """
        Provide task_instance context to airflow task handler.

        :param ti: task instance object
        """
        local_loc = self._init_file(ti)
        self.handler = NonCachingFileHandler(local_loc, encoding='utf-8')
        if self.formatter:
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

    def _render_filename(self, ti: TaskInstance, try_number: int) -> str:
        with create_session() as session:
            dag_run = ti.get_dagrun(session=session)
            template = dag_run.get_log_template(session=session).filename
        str_tpl, jinja_tpl = parse_template_string(template)

        if jinja_tpl:
            if hasattr(ti, "task"):
                context = ti.get_template_context()
            else:
                context = Context(ti=ti, ts=dag_run.logical_date.isoformat())
            context["try_number"] = try_number
            return render_template_to_string(jinja_tpl, context)
        elif str_tpl:
            try:
                dag = ti.task.dag
            except AttributeError:  # ti.task is not always set.
                data_interval = (dag_run.data_interval_start, dag_run.data_interval_end)
            else:
                if TYPE_CHECKING:
                    assert dag is not None
                data_interval = dag.get_run_data_interval(dag_run)
            if data_interval[0]:
                data_interval_start = data_interval[0].isoformat()
            else:
                data_interval_start = ""
            if data_interval[1]:
                data_interval_end = data_interval[1].isoformat()
            else:
                data_interval_end = ""
            return str_tpl.format(
                dag_id=ti.dag_id,
                task_id=ti.task_id,
                run_id=ti.run_id,
                data_interval_start=data_interval_start,
                data_interval_end=data_interval_end,
                execution_date=ti.get_dagrun().logical_date.isoformat(),
                try_number=try_number,
            )
        else:
            raise RuntimeError(f"Unable to render log filename for {ti}. This should never happen")

    def _read_grouped_logs(self):
        return False

    def _read(self, ti: TaskInstance, try_number: int, metadata: dict[str, Any] | None = None):
        """
        Template method that contains custom logic of reading
        logs given the try_number.

        :param ti: task instance record
        :param try_number: current try_number to read log from
        :param metadata: log metadata,
                         can be used for steaming log reading and auto-tailing.
                         Following attributes are used:
                         log_pos: (absolute) Char position to which the log
                                  which was retrieved in previous calls, this
                                  part will be skipped and only following test
                                  returned to be added to tail.

        :return: log message as a string and metadata.
                 Following attributes are used in metadata:
                 end_of_log: Boolean, True if end of log is reached or False
                             if further calls might get more log text.
                             This is determined by the status of the TaskInstance
                 log_pos: (absolute) Char position to which the log is retrieved
        """
        from airflow.utils.jwt_signer import JWTSigner

        # Task instance here might be different from task instance when
        # initializing the handler. Thus explicitly getting log location
        # is needed to get correct log path.
        log_relative_path = self._render_filename(ti, try_number)
        location = os.path.join(self.local_base, log_relative_path)

        log = ""

        if os.path.exists(location):
            try:
                with open(location, encoding="utf-8", errors="surrogateescape") as file:
                    log += f"*** Reading local file: {location}\n"
                    log += "".join(file.readlines())
            except Exception as e:
                log = f"*** Failed to load local log file: {location}\n"
                log += f"*** {str(e)}\n"
                return log, {'end_of_log': True}
        elif conf.get('core', 'executor') == 'KubernetesExecutor':
            try:
                from airflow.kubernetes.kube_client import get_kube_client

                kube_client = get_kube_client()

                if len(ti.hostname) >= 63:
                    # Kubernetes takes the pod name and truncates it for the hostname. This truncated hostname
                    # is returned for the fqdn to comply with the 63 character limit imposed by DNS standards
                    # on any label of a FQDN.
                    pod_list = kube_client.list_namespaced_pod(conf.get('kubernetes', 'namespace'))
                    matches = [
                        pod.metadata.name
                        for pod in pod_list.items
                        if pod.metadata.name.startswith(ti.hostname)
                    ]
                    if len(matches) == 1:
                        if len(matches[0]) > len(ti.hostname):
                            ti.hostname = matches[0]

                log += f'*** Trying to get logs (last 100 lines) from worker pod {ti.hostname} ***\n\n'

                res = kube_client.read_namespaced_pod_log(
                    name=ti.hostname,
                    namespace=conf.get('kubernetes', 'namespace'),
                    container='base',
                    follow=False,
                    tail_lines=100,
                    _preload_content=False,
                )

                for line in res:
                    log += line.decode()

            except Exception as f:
                log += f'*** Unable to fetch logs from worker pod {ti.hostname} ***\n{str(f)}\n\n'
                return log, {'end_of_log': True}
        else:
            import httpx

            url = urljoin(
                f"http://{ti.hostname}:{conf.get('logging', 'WORKER_LOG_SERVER_PORT')}/log/",
                log_relative_path,
            )
            log += f"*** Log file does not exist: {location}\n"
            log += f"*** Fetching from: {url}\n"
            try:
                timeout = None  # No timeout
                try:
                    timeout = conf.getint('webserver', 'log_fetch_timeout_sec')
                except (AirflowConfigException, ValueError):
                    pass

                signer = JWTSigner(
                    secret_key=conf.get('webserver', 'secret_key'),
                    expiration_time_in_seconds=conf.getint(
                        'webserver', 'log_request_clock_grace', fallback=30
                    ),
                    audience="task-instance-logs",
                )
                response = httpx.get(
                    url,
                    timeout=timeout,
                    headers={'Authorization': signer.generate_signed_token({"filename": log_relative_path})},
                )
                response.encoding = "utf-8"

                if response.status_code == 403:
                    log += (
                        "*** !!!! Please make sure that all your Airflow components (e.g. "
                        "schedulers, webservers and workers) have "
                        "the same 'secret_key' configured in 'webserver' section and "
                        "time is synchronized on all your machines (for example with ntpd) !!!!!\n***"
                    )
                    log += (
                        "*** See more at https://airflow.apache.org/docs/apache-airflow/"
                        "stable/configurations-ref.html#secret-key\n***"
                    )
                # Check if the resource was properly fetched
                response.raise_for_status()

                log += '\n' + response.text
            except Exception as e:
                log += f"*** Failed to fetch log file from worker. {str(e)}\n"
                return log, {'end_of_log': True}

        # Process tailing if log is not at it's end
        end_of_log = ti.try_number != try_number or ti.state not in State.running
        log_pos = len(log)
        if metadata and 'log_pos' in metadata:
            previous_chars = metadata['log_pos']
            log = log[previous_chars:]  # Cut off previously passed log test as new tail

        return log, {'end_of_log': end_of_log, 'log_pos': log_pos}

    def read(self, task_instance, try_number=None, metadata=None):
        """
        Read logs of given task instance from local machine.

        :param task_instance: task instance object
        :param try_number: task instance try_number to read logs from. If None
                           it returns all logs separated by try_number
        :param metadata: log metadata,
                         can be used for steaming log reading and auto-tailing.
        :return: a list of listed tuples which order log string by host
        """
        # Task instance increments its try number when it starts to run.
        # So the log for a particular task try will only show up when
        # try number gets incremented in DB, i.e logs produced the time
        # after cli run and before try_number + 1 in DB will not be displayed.

        if try_number is None:
            next_try = task_instance.next_try_number
            try_numbers = list(range(1, next_try))
        elif try_number < 1:
            logs = [
                [('default_host', f'Error fetching the logs. Try number {try_number} is invalid.')],
            ]
            return logs, [{'end_of_log': True}]
        else:
            try_numbers = [try_number]

        logs = [''] * len(try_numbers)
        metadata_array = [{}] * len(try_numbers)
        for i, try_number_element in enumerate(try_numbers):
            log, out_metadata = self._read(task_instance, try_number_element, metadata)
            # es_task_handler return logs grouped by host. wrap other handler returning log string
            # with default/ empty host so that UI can render the response in the same way
            logs[i] = log if self._read_grouped_logs() else [(task_instance.hostname, log)]
            metadata_array[i] = out_metadata

        return logs, metadata_array

    def _init_file(self, ti):
        """
        Create log directory and give it correct permissions.

        :param ti: task instance object
        :return: relative log path of the given task instance
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
        relative_path = self._render_filename(ti, ti.try_number)
        full_path = os.path.join(self.local_base, relative_path)
        directory = os.path.dirname(full_path)
        # Create the log file and give it group writable permissions
        # TODO(aoen): Make log dirs and logs globally readable for now since the SubDag
        # operator is not compatible with impersonation (e.g. if a Celery executor is used
        # for a SubDag operator and the SubDag operator has a different owner than the
        # parent DAG)
        Path(directory).mkdir(mode=0o777, parents=True, exist_ok=True)

        if not os.path.exists(full_path):
            open(full_path, "a").close()
            # TODO: Investigate using 444 instead of 666.
            try:
                os.chmod(full_path, 0o666)
            except OSError:
                logging.warning("OSError while change ownership of the log file")

        return full_path
