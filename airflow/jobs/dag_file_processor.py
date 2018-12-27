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
#
# from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import logging
import multiprocessing
import os
import signal
import sys
import threading
import time

from airflow import LoggingMixin, settings, AirflowException
from airflow.jobs.scheduler_job import SchedulerJob
from airflow.utils import timezone
from airflow.utils.dag_processing import AbstractDagFileProcessor
from airflow.utils.log.logging_mixin import StreamLogWriter, set_context


class DagFileProcessor(AbstractDagFileProcessor, LoggingMixin):
    """Helps call SchedulerJob.process_file() in a separate process."""

    # Counter that increments everytime an instance of this class is created
    class_creation_counter = 0

    def __init__(self, file_path, pickle_dags, dag_id_white_list, zombies):
        """
        :param file_path: a Python file containing Airflow DAG definitions
        :type file_path: unicode
        :param pickle_dags: whether to serialize the DAG objects to the DB
        :type pickle_dags: bool
        :param dag_id_whitelist: If specified, only look at these DAG ID's
        :type dag_id_whitelist: list[unicode]
        :param zombies: zombie task instances to kill
        :type zombies: list[SimpleTaskInstance]
        """
        self._file_path = file_path
        # Queue that's used to pass results from the child process.
        self._result_queue = multiprocessing.Queue()
        # The process that was launched to process the given .
        self._process = None
        self._dag_id_white_list = dag_id_white_list
        self._pickle_dags = pickle_dags
        self._zombies = zombies
        # The result of Scheduler.process_file(file_path).
        self._result = None
        # Whether the process is done running.
        self._done = False
        # When the process started.
        self._start_time = None
        # This ID is use to uniquely name the process / thread that's launched
        # by this processor instance
        self._instance_id = DagFileProcessor.class_creation_counter
        DagFileProcessor.class_creation_counter += 1

    @property
    def file_path(self):
        return self._file_path

    @staticmethod
    def _launch_process(result_queue,
                        file_path,
                        pickle_dags,
                        dag_id_white_list,
                        thread_name,
                        zombies):
        """
        Launch a process to process the given file.

        :param result_queue: the queue to use for passing back the result
        :type result_queue: multiprocessing.Queue
        :param file_path: the file to process
        :type file_path: unicode
        :param pickle_dags: whether to pickle the DAGs found in the file and
            save them to the DB
        :type pickle_dags: bool
        :param dag_id_white_list: if specified, only examine DAG ID's that are
            in this list
        :type dag_id_white_list: list[unicode]
        :param thread_name: the name to use for the process that is launched
        :type thread_name: unicode
        :return: the process that was launched
        :rtype: multiprocessing.Process
        :param zombies: zombie task instances to kill
        :type zombies: list[SimpleTaskInstance]
        """
        def helper():
            # This helper runs in the newly created process
            log = logging.getLogger("airflow.processor")

            stdout = StreamLogWriter(log, logging.INFO)
            stderr = StreamLogWriter(log, logging.WARN)

            set_context(log, file_path)

            try:
                # redirect stdout/stderr to log
                sys.stdout = stdout
                sys.stderr = stderr

                # Re-configure the ORM engine as there are issues with multiple processes
                settings.configure_orm()

                # Change the thread name to differentiate log lines. This is
                # really a separate process, but changing the name of the
                # process doesn't work, so changing the thread name instead.
                threading.current_thread().name = thread_name
                start_time = time.time()

                log.info("Started process (PID=%s) to work on %s",
                         os.getpid(), file_path)
                scheduler_job = SchedulerJob(dag_ids=dag_id_white_list, log=log)
                result = scheduler_job.process_file(file_path,
                                                    zombies,
                                                    pickle_dags)
                result_queue.put(result)
                end_time = time.time()
                log.info(
                    "Processing %s took %.3f seconds", file_path, end_time - start_time
                )
            except Exception:
                # Log exceptions through the logging framework.
                log.exception("Got an exception! Propagating...")
                raise
            finally:
                sys.stdout = sys.__stdout__
                sys.stderr = sys.__stderr__
                # We re-initialized the ORM within this Process above so we need to
                # tear it down manually here
                settings.dispose_orm()

        p = multiprocessing.Process(target=helper,
                                    args=(),
                                    name="{}-Process".format(thread_name))
        p.start()
        return p

    def start(self):
        """
        Launch the process and start processing the DAG.
        """
        self._process = DagFileProcessor._launch_process(
            self._result_queue,
            self.file_path,
            self._pickle_dags,
            self._dag_id_white_list,
            "DagFileProcessor{}".format(self._instance_id),
            self._zombies)
        self._start_time = timezone.utcnow()

    def terminate(self, sigkill=False):
        """
        Terminate (and then kill) the process launched to process the file.

        :param sigkill: whether to issue a SIGKILL if SIGTERM doesn't work.
        :type sigkill: bool
        """
        if self._process is None:
            raise AirflowException("Tried to call stop before starting!")
        # The queue will likely get corrupted, so remove the reference
        self._result_queue = None
        self._process.terminate()
        # Arbitrarily wait 5s for the process to die
        self._process.join(5)
        if sigkill and self._process.is_alive():
            self.log.warning("Killing PID %s", self._process.pid)
            os.kill(self._process.pid, signal.SIGKILL)

    @property
    def pid(self):
        """
        :return: the PID of the process launched to process the given file
        :rtype: int
        """
        if self._process is None:
            raise AirflowException("Tried to get PID before starting!")
        return self._process.pid

    @property
    def exit_code(self):
        """
        After the process is finished, this can be called to get the return code

        :return: the exit code of the process
        :rtype: int
        """
        if not self._done:
            raise AirflowException("Tried to call retcode before process was finished!")
        return self._process.exitcode

    @property
    def done(self):
        """
        Check if the process launched to process this file is done.

        :return: whether the process is finished running
        :rtype: bool
        """
        if self._process is None:
            raise AirflowException("Tried to see if it's done before starting!")

        if self._done:
            return True

        # In case result queue is corrupted.
        if self._result_queue and not self._result_queue.empty():
            self._result = self._result_queue.get_nowait()
            self._done = True
            self.log.debug("Waiting for %s", self._process)
            self._process.join()
            return True

        # Potential error case when process dies
        if self._result_queue and not self._process.is_alive():
            self._done = True
            # Get the object from the queue or else join() can hang.
            if not self._result_queue.empty():
                self._result = self._result_queue.get_nowait()
            self.log.debug("Waiting for %s", self._process)
            self._process.join()
            return True

        return False

    @property
    def result(self):
        """
        :return: result of running SchedulerJob.process_file()
        :rtype: SimpleDag
        """
        if not self.done:
            raise AirflowException("Tried to get the result before it's done!")
        return self._result

    @property
    def start_time(self):
        """
        :return: when this started to process the file
        :rtype: datetime
        """
        if self._start_time is None:
            raise AirflowException("Tried to get start time before it started!")
        return self._start_time
