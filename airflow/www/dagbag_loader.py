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
import copy
import json
import logging
import os
import pickle
import signal
import six
import sys
import threading
import time
from collections import defaultdict
from multiprocessing import Event
from multiprocessing import Process
from multiprocessing import Queue

from airflow import configuration
from airflow import models
from airflow import settings
from airflow.www.utils import get_python_source


def _read_config(field, default):
    try:
        return configuration.getint('webserver', field)
    except:
        return default


# Interval to send dagbag back to main process.
DAGBAG_SYNC_INTERVAL = _read_config('dagbag_sync_interval', 10)
COLLECT_DAGS_INTERVAL = _read_config('collect_dags_interval', 30)


class _DagBag(models.DagBag):
    """
    A wrapper of models.DagBag without calling collect_dags during initialization.
    """

    def __init__(
        self,
        dag_folder=None,
        store_serialized_dags=False
    ):
        # do not use default arg in signature, to fix import cycle on plugin load
        dag_folder = dag_folder or settings.DAGS_FOLDER
        self.log.info("Filling up the DagBag from %s", dag_folder)
        self.dag_folder = dag_folder
        self.dags = {}
        # the file's last modified timestamp when we last read it
        self.file_last_changed = {}
        self.executor = None
        self.import_errors = {}
        self.has_logged = False
        self.store_serialized_dags = store_serialized_dags


def _kill_proc(dummy_signum, dummy_frame):
    logging.info('Asynchronous Dagbag Loader exiting.')
    sys.exit(0)


def _create_dagbag(dag_folder, queue):
    """A process that creates, updates, and sync dagbag in background."""

    _primitive_types = set([int, bool, float, str, bytes, six.string_types])

    # Stringify all fields of DAGs and tasks except for time related fields.
    # 'parent_dag' and '_dag' are not stringified because they link to a DAG already stringified,
    # stringifying them leads to a deadlock loop.
    _dag_fields_to_keep = set(['schedule_interval', 'start_date', 'end_date', 'dagrun_timeout',
                               'timezone', 'last_loaded', '_schedule_interval', 'parent_dag'])

    _task_fields_to_keep = set([
        'retry_delay', 'max_retry_delay', 'start_date', 'end_date', 'schedule_interval', 'sla',
        'execution_timeout', 'dag', '_dag'])

    def _stringify(x):
        """Stringify anything, the order is roughly based on their occuring frequency."""
        try:
            if x is None or type(x) in _primitive_types or (six.PY2 and type(x) is unicode):
                return x
            elif isinstance(x, dict):
                return {k: _stringify(v) for k, v in x.items()}
            elif isinstance(x, models.DAG):
                return _stringify_object(x, _dag_fields_to_keep)
            elif isinstance(x, models.BaseOperator):
                return _stringify_object(x, _task_fields_to_keep)
            elif callable(x):
                return get_python_source(x)
            elif isinstance(x, list):
                return [_stringify(v) for v in x]
            elif isinstance(x, set):
                return set([_stringify(v) for v in x])
            elif isinstance(x, tuple):
                return tuple([_stringify(v) for v in x])
            else:
                return str(x)
        except:
            logging.warning('Failed to stringify.', exc_info=True)
            return 'failed_to_stringify'

    def _stringify_object(x, fields_to_keep):
        """Stringify a DAG or a task inplace."""
        for k, v in x.__dict__.items():
            if k not in fields_to_keep:
                x.__dict__[k] = _stringify(v)
        return x

    def _send_dagbag(dagbag, queue, event_collect_done, event_next_collect):
        """A thread that sends dags."""
        dagbag = {
            'dags': dagbag.dags,
            'file_last_changed': dagbag.file_last_changed,
            'import_errors': dagbag.import_errors
        }
        previous_keys = defaultdict(set)
        while True:
            try:
                collect_done = event_collect_done.is_set()

                dagbag_update = {}
                for k, v in dagbag.items():
                    current_keys = set(copy.deepcopy(list(v.keys())))
                    new_keys = current_keys - previous_keys[k]
                    previous_keys[k] = set() if collect_done else current_keys
                    if new_keys:
                        if k == 'dags':
                            tmp_dags = {}
                            for x in new_keys:
                                try:
                                    tmp_dag = copy.deepcopy(_stringify(v[x]))
                                    _ = pickle.dumps(tmp_dag, protocol=pickle.HIGHEST_PROTOCOL)
                                    tmp_dags[x] = tmp_dag
                                except:
                                    logging.warning('DAG {} can not be pickled.'.format(x),
                                                    exc_info=True)
                            dagbag_update[k] = tmp_dags
                        else:
                            dagbag_update[k] = {x: v[x] for x in new_keys}

                if dagbag_update or collect_done:
                    queue.put((collect_done, dagbag_update))

                if collect_done:
                    for v in dagbag.values():
                        v.clear()

                    event_collect_done.clear()
                    event_next_collect.set()

                time.sleep(DAGBAG_SYNC_INTERVAL)
            except:
                logging.warning('Dagbag loader sender errors.', exc_info=True)

    import airflow
    from airflow import configuration
    try:
        with open('/home/airflow/gcs/env_var.json', 'r') as env_var_json:
            os.environ.update(json.load(env_var_json))
    except:
        logging.warning('Using default Composer Environment Variables. Overrides '
                        'have not been applied.')
    configuration = six.moves.reload_module(configuration)
    airflow.configuration = six.moves.reload_module(airflow.configuration)
    airflow.plugins_manager = six.moves.reload_module(airflow.plugins_manager)
    airflow = six.moves.reload_module(airflow)

    signal.signal(signal.SIGTERM, _kill_proc)
    signal.signal(signal.SIGINT, _kill_proc)

    logging.info('Using Asynchronous Dagbag Loader.')
    dagbag = _DagBag(dag_folder)
    event_collect_done = Event()
    event_next_collect = Event()
    thread = threading.Thread(target=_send_dagbag, args=(dagbag, queue, event_collect_done,
                                                         event_next_collect))
    thread.daemon = True
    thread.start()
    while True:
        try:
            event_collect_done.clear()
            event_next_collect.clear()

            start_time = time.time()
            dagbag.collect_dags(dag_folder,
                                include_examples=configuration.getboolean('core', 'LOAD_EXAMPLES'))
            event_collect_done.set()
            event_next_collect.wait()

            time.sleep(max(0, COLLECT_DAGS_INTERVAL - (time.time() - start_time)))
        except SystemExit:
            sys.exit(0)
        except:
            logging.warning('Dagbag loader dags collector errors.', exc_info=True)


def create_async_dagbag(dag_folder):
    """
    It creates a new process to collect DAGs with a sender thread puts updated DAGs
    on a queue. The main process creates a receiver thread to get DAGs and update
    the DagBag. All new processe and threads are daemon so the main process never
    waits for them at exiting time.
    """
    def _receive_dagbag(dagbag, queue):
        """A thread that receives updated dagbag."""
        previous_keys = defaultdict(list)
        while True:
            try:
                collect_done, dagbag_update = queue.get()
                for k, v in dagbag_update.items():
                    previous_keys[k].extend(v.keys())
                    dagbag.__dict__[k].update(v)
                if collect_done:
                    for k in ['dags', 'file_last_changed', 'import_errors']:
                        v = dagbag.__dict__[k]
                        for key_to_delete in set(v.keys()) - set(previous_keys[k]):
                            del v[key_to_delete]
                        previous_keys[k] = []
            except:
                logging.warning('Dagbag loader receiver errors.', exc_info=True)

    dagbag = _DagBag(dag_folder)
    queue = Queue(maxsize=1)
    process = Process(target=_create_dagbag, args=(dag_folder, queue))
    process.daemon = True
    thread = threading.Thread(target=_receive_dagbag, args=(dagbag, queue))
    thread.daemon = True
    process.start()
    thread.start()
    return dagbag
