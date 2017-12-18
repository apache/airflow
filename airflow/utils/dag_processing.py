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
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import os
import re
import time
import zipfile
from abc import ABCMeta, abstractmethod
from collections import defaultdict

from airflow.dag.base_dag import BaseDag, BaseDagBag
from airflow.exceptions import AirflowException
from airflow.utils import timezone
from airflow.utils.log.logging_mixin import LoggingMixin


class SimpleDag(BaseDag):
    """
    A simplified representation of a DAG that contains all attributes
    required for instantiating and scheduling its associated tasks.
    """

    def __init__(self, dag, pickle_id=None):
        """
        :param dag: the DAG
        :type dag: DAG
        :param pickle_id: ID associated with the pickled version of this DAG.
        :type pickle_id: unicode
        """
        self._dag_id = dag.dag_id
        self._task_ids = [task.task_id for task in dag.tasks]
        self._full_filepath = dag.full_filepath
        self._is_paused = dag.is_paused
        self._concurrency = dag.concurrency
        self._pickle_id = pickle_id
        self._task_special_args = {}
        for task in dag.tasks:
            special_args = {}
            if task.task_concurrency is not None:
                special_args['task_concurrency'] = task.task_concurrency
            if len(special_args) > 0:
                self._task_special_args[task.task_id] = special_args

    @property
    def dag_id(self):
        """
        :return: the DAG ID
        :rtype: unicode
        """
        return self._dag_id

    @property
    def task_ids(self):
        """
        :return: A list of task IDs that are in this DAG
        :rtype: list[unicode]
        """
        return self._task_ids

    @property
    def full_filepath(self):
        """
        :return: The absolute path to the file that contains this DAG's definition
        :rtype: unicode
        """
        return self._full_filepath

    @property
    def concurrency(self):
        """
        :return: maximum number of tasks that can run simultaneously from this DAG
        :rtype: int
        """
        return self._concurrency

    @property
    def is_paused(self):
        """
        :return: whether this DAG is paused or not
        :rtype: bool
        """
        return self._is_paused

    @property
    def pickle_id(self):
        """
        :return: The pickle ID for this DAG, if it has one. Otherwise None.
        :rtype: unicode
        """
        return self._pickle_id

    @property
    def task_special_args(self):
        return self._task_special_args

    def get_task_special_arg(self, task_id, special_arg_name):
        if task_id in self._task_special_args and special_arg_name in self._task_special_args[task_id]:
            return self._task_special_args[task_id][special_arg_name]
        else:
            return None


class SimpleDagBag(BaseDagBag):
    """
    A collection of SimpleDag objects with some convenience methods.
    """

    def __init__(self, simple_dags):
        """
        Constructor.

        :param simple_dags: SimpleDag objects that should be in this
        :type: list(SimpleDag)
        """
        self.simple_dags = simple_dags
        self.dag_id_to_simple_dag = {}

        for simple_dag in simple_dags:
            self.dag_id_to_simple_dag[simple_dag.dag_id] = simple_dag

    @property
    def dag_ids(self):
        """
        :return: IDs of all the DAGs in this
        :rtype: list[unicode]
        """
        return self.dag_id_to_simple_dag.keys()

    def get_dag(self, dag_id):
        """
        :param dag_id: DAG ID
        :type dag_id: unicode
        :return: if the given DAG ID exists in the bag, return the BaseDag
        corresponding to that ID. Otherwise, throw an Exception
        :rtype: SimpleDag
        """
        if dag_id not in self.dag_id_to_simple_dag:
            raise AirflowException("Unknown DAG ID {}".format(dag_id))
        return self.dag_id_to_simple_dag[dag_id]


def list_py_file_paths(directory, safe_mode=True):
    """
    Traverse a directory and look for Python files.

    :param directory: the directory to traverse
    :type directory: unicode
    :param safe_mode: whether to use a heuristic to determine whether a file
    contains Airflow DAG definitions
    :return: a list of paths to Python files in the specified directory
    :rtype: list[unicode]
    """
    file_paths = []
    if directory is None:
        return []
    elif os.path.isfile(directory):
        return [directory]
    elif os.path.isdir(directory):
        patterns = []
        for root, dirs, files in os.walk(directory, followlinks=True):
            ignore_file = [f for f in files if f == '.airflowignore']
            if ignore_file:
                f = open(os.path.join(root, ignore_file[0]), 'r')
                patterns += [p for p in f.read().split('\n') if p]
                f.close()
            for f in files:
                try:
                    file_path = os.path.join(root, f)
                    if not os.path.isfile(file_path):
                        continue
                    mod_name, file_ext = os.path.splitext(
                        os.path.split(file_path)[-1])
                    if file_ext != '.py' and not zipfile.is_zipfile(file_path):
                        continue
                    if any([re.findall(p, file_path) for p in patterns]):
                        continue

                    # Heuristic that guesses whether a Python file contains an
                    # Airflow DAG definition.
                    might_contain_dag = True
                    if safe_mode and not zipfile.is_zipfile(file_path):
                        with open(file_path, 'rb') as f:
                            content = f.read()
                            might_contain_dag = all(
                                [s in content for s in (b'DAG', b'airflow')])

                    if not might_contain_dag:
                        continue

                    file_paths.append(file_path)
                except Exception:
                    log = LoggingMixin().log
                    log.exception("Error while examining %s", f)
    return file_paths


class AbstractDagFileProcessor(object):
    """
    Processes a DAG file. See SchedulerJob.process_file() for more details.
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def start(self):
        """
        Launch the process to process the file
        """
        raise NotImplementedError()

    @abstractmethod
    def terminate(self, sigkill=False):
        """
        Terminate (and then kill) the process launched to process the file
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def pid(self):
        """
        :return: the PID of the process launched to process the given file
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def exit_code(self):
        """
        After the process is finished, this can be called to get the return code
        :return: the exit code of the process
        :rtype: int
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def done(self):
        """
        Check if the process launched to process this file is done.
        :return: whether the process is finished running
        :rtype: bool
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def result(self):
        """
        :return: result of running SchedulerJob.process_file()
        :rtype: list[SimpleDag]
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def start_time(self):
        """
        :return: When this started to process the file
        :rtype: datetime
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def file_path(self):
        """
        :return: the path to the file that this is processing
        :rtype: unicode
        """
        raise NotImplementedError()


class DagFileProcessorManager(LoggingMixin):
    """
    Given a list of DAG definition files, this kicks off several processors
    in parallel to process them. The parallelism is limited and as the
    processors finish, more are launched. The files are processed over and
    over again, but no more often than the specified interval.

    :type _file_path_queue: list[unicode]
    :type _processors: dict[unicode, AbstractDagFileProcessor]
    :type _last_runtime: dict[unicode, float]
    :type _last_finish_time: dict[unicode, datetime]
    """

    def __init__(self,
                 dag_directory,
                 file_paths,
                 parallelism,
                 process_file_interval,
                 max_runs,
                 processor_factory):
        """
        :param dag_directory: Directory where DAG definitions are kept. All
        files in file_paths should be under this directory
        :type dag_directory: unicode
        :param file_paths: list of file paths that contain DAG definitions
        :type file_paths: list[unicode]
        :param parallelism: maximum number of simultaneous process to run at once
        :type parallelism: int
        :param process_file_interval: process a file at most once every this
        many seconds
        :type process_file_interval: float
        :param max_runs: The number of times to parse and schedule each file. -1
        for unlimited.
        :type max_runs: int
        :type process_file_interval: float
        :param processor_factory: function that creates processors for DAG
        definition files. Arguments are (dag_definition_path)
        :type processor_factory: (unicode, unicode) -> (AbstractDagFileProcessor)

        """
        self._file_paths = file_paths
        self._file_path_queue = []
        self._parallelism = parallelism
        self._dag_directory = dag_directory
        self._max_runs = max_runs
        self._process_file_interval = process_file_interval
        self._processor_factory = processor_factory
        # Map from file path to the processor
        self._processors = {}
        # Map from file path to the last runtime
        self._last_runtime = {}
        # Map from file path to the last finish time
        self._last_finish_time = {}
        # Map from file path to the number of runs
        self._run_count = defaultdict(int)
        # Scheduler heartbeat key.
        self._heart_beat_key = 'heart-beat'

    @property
    def file_paths(self):
        return self._file_paths

    def get_pid(self, file_path):
        """
        :param file_path: the path to the file that's being processed
        :type file_path: unicode
        :return: the PID of the process processing the given file or None if
        the specified file is not being processed
        :rtype: int
        """
        if file_path in self._processors:
            return self._processors[file_path].pid
        return None

    def get_all_pids(self):
        """
        :return: a list of the PIDs for the processors that are running
        :rtype: List[int]
        """
        return [x.pid for x in self._processors.values()]

    def get_runtime(self, file_path):
        """
        :param file_path: the path to the file that's being processed
        :type file_path: unicode
        :return: the current runtime (in seconds) of the process that's
        processing the specified file or None if the file is not currently
        being processed
        """
        if file_path in self._processors:
            return (timezone.utcnow() - self._processors[file_path].start_time)\
                .total_seconds()
        return None

    def get_last_runtime(self, file_path):
        """
        :param file_path: the path to the file that was processed
        :type file_path: unicode
        :return: the runtime (in seconds) of the process of the last run, or
        None if the file was never processed.
        :rtype: float
        """
        return self._last_runtime.get(file_path)

    def get_last_finish_time(self, file_path):
        """
        :param file_path: the path to the file that was processed
        :type file_path: unicode
        :return: the finish time of the process of the last run, or None if the
        file was never processed.
        :rtype: datetime
        """
        return self._last_finish_time.get(file_path)

    def get_start_time(self, file_path):
        """
        :param file_path: the path to the file that's being processed
        :type file_path: unicode
        :return: the start time of the process that's processing the
        specified file or None if the file is not currently being processed
        :rtype: datetime
        """
        if file_path in self._processors:
            return self._processors[file_path].start_time
        return None

    def set_file_paths(self, new_file_paths):
        """
        Update this with a new set of paths to DAG definition files.

        :param new_file_paths: list of paths to DAG definition files
        :type new_file_paths: list[unicode]
        :return: None
        """
        self._file_paths = new_file_paths
        self._file_path_queue = [x for x in self._file_path_queue
                                 if x in new_file_paths]
        # Stop processors that are working on deleted files
        filtered_processors = {}
        for file_path, processor in self._processors.items():
            if file_path in new_file_paths:
                filtered_processors[file_path] = processor
            else:
                self.log.warning("Stopping processor for %s", file_path)
                processor.terminate()
        self._processors = filtered_processors

    def processing_count(self):
        """
        :return: the number of files currently being processed
        :rtype: int
        """
        return len(self._processors)

    def wait_until_finished(self):
        """
        Sleeps until all the processors are done.
        """
        for file_path, processor in self._processors.items():
            while not processor.done:
                time.sleep(0.1)

    def heartbeat(self):
        """
        This should be periodically called by the scheduler. This method will
        kick of new processes to process DAG definition files and read the
        results from the finished processors.

        :return: a list of SimpleDags that were produced by processors that
        have finished since the last time this was called
        :rtype: list[SimpleDag]
        """
        finished_processors = {}
        """:type : dict[unicode, AbstractDagFileProcessor]"""
        running_processors = {}
        """:type : dict[unicode, AbstractDagFileProcessor]"""

        for file_path, processor in self._processors.items():
            if processor.done:
                self.log.info("Processor for %s finished", file_path)
                now = timezone.utcnow()
                finished_processors[file_path] = processor
                self._last_runtime[file_path] = (now -
                                                 processor.start_time).total_seconds()
                self._last_finish_time[file_path] = now
                self._run_count[file_path] += 1
            else:
                running_processors[file_path] = processor
        self._processors = running_processors

        # Collect all the DAGs that were found in the processed files
        simple_dags = []
        for file_path, processor in finished_processors.items():
            if processor.result is None:
                self.log.warning(
                    "Processor for %s exited with return code %s.",
                    processor.file_path, processor.exit_code
                )
            else:
                for simple_dag in processor.result:
                    simple_dags.append(simple_dag)

        # Generate more file paths to process if we processed all the files
        # already.
        if len(self._file_path_queue) == 0:
            # If the file path is already being processed, or if a file was
            # processed recently, wait until the next batch
            file_paths_in_progress = self._processors.keys()
            now = timezone.utcnow()
            file_paths_recently_processed = []
            for file_path in self._file_paths:
                last_finish_time = self.get_last_finish_time(file_path)
                if (last_finish_time is not None and
                    (now - last_finish_time).total_seconds() <
                        self._process_file_interval):
                    file_paths_recently_processed.append(file_path)

            files_paths_at_run_limit = [file_path
                                        for file_path, num_runs in self._run_count.items()
                                        if num_runs == self._max_runs]

            files_paths_to_queue = list(set(self._file_paths) -
                                        set(file_paths_in_progress) -
                                        set(file_paths_recently_processed) -
                                        set(files_paths_at_run_limit))

            for file_path, processor in self._processors.items():
                self.log.debug(
                    "File path %s is still being processed (started: %s)",
                    processor.file_path, processor.start_time.isoformat()
                )

            self.log.debug(
                "Queuing the following files for processing:\n\t%s",
                "\n\t".join(files_paths_to_queue)
            )

            self._file_path_queue.extend(files_paths_to_queue)

        # Start more processors if we have enough slots and files to process
        while (self._parallelism - len(self._processors) > 0 and
               len(self._file_path_queue) > 0):
            file_path = self._file_path_queue.pop(0)
            processor = self._processor_factory(file_path)

            processor.start()
            self.log.info(
                "Started a process (PID: %s) to generate tasks for %s",
                processor.pid, file_path
            )
            self._processors[file_path] = processor

        # Update scheduler heartbeat count.
        self._run_count[self._heart_beat_key] += 1

        return simple_dags

    def max_runs_reached(self):
        """
        :return: whether all file paths have been processed max_runs times
        """
        if self._max_runs == -1:  # Unlimited runs.
            return False
        for file_path in self._file_paths:
            if self._run_count[file_path] != self._max_runs:
                return False
        if self._run_count[self._heart_beat_key] < self._max_runs:
            return False
        return True

    def terminate(self):
        """
        Stops all running processors
        :return: None
        """
        for processor in self._processors.values():
            processor.terminate()
