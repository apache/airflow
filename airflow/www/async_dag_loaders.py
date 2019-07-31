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

"""Asynchronous DAG loader that collects DAGs in background.

broad-except is used in many places to capture all exceptions, to
make sure when loading one DAG fails, it does not affect other DAGs.
"""

import copy
import importlib
import logging
import signal
import sys
import threading
import time
from collections import defaultdict
from multiprocessing import Event
from multiprocessing import Process
from multiprocessing import Queue

from airflow import configuration
from airflow.dag.serialization import deserialize
from airflow.dag.serialization import serialize
from airflow.models import DagBag


# Interval to send dagbag back to main process.
DAGBAG_SYNC_INTERVAL = configuration.getint(
    'webserver', 'dagbag_sync_interval', fallback=10)
COLLECT_DAGS_INTERVAL = configuration.getint(
    'webserver', 'collect_dags_interval', fallback=30)


# pylint: disable=unused-argument
def _kill_proc(dummy_signum, dummy_frame):
    logging.info('Asynchronous Dagbag Loader exiting.')
    sys.exit(0)


def _serialize_dags(dags, dag_ids):
    serialized_dags = {}
    for dag_id in dag_ids:
        try:
            serialized_dags[dag_id] = serialize(dags[dag_id])
        except Exception:  # pylint: disable=broad-except
            logging.warning('DAG %s can not be serialized.', dag_id,
                            exc_info=True)
    return serialized_dags


def _make_dagbag_update(dagbag, previous_keys, collect_done):
    dagbag_update = {}
    for k, v in dagbag.items():
        current_keys = set(copy.deepcopy(list(v.keys())))
        new_keys = current_keys - previous_keys[k]
        previous_keys[k] = set() if collect_done else current_keys
        if new_keys:
            dagbag_update[k] = (
                _serialize_dags(v, new_keys) if k == 'dags'
                else {x: v[x] for x in new_keys})
    return dagbag_update


def _send_dagbag(dagbag, queue, event_collect_done, event_next_collect):
    """A thread that sends dags."""
    dagbag = {
        'dags': dagbag.dags,
        'file_last_changed': dagbag.file_last_changed,
        'import_errors': dagbag.import_errors
    }
    previous_keys = defaultdict(set)
    while True:  # pylint: disable=too-many-nested-blocks
        try:
            collect_done = event_collect_done.is_set()
            dagbag_update = _make_dagbag_update(
                dagbag, previous_keys, collect_done)

            if dagbag_update or collect_done:
                queue.put((collect_done, dagbag_update))

            if collect_done:
                for v in dagbag.values():
                    v.clear()

                event_collect_done.clear()
                event_next_collect.set()

            time.sleep(DAGBAG_SYNC_INTERVAL)
        except Exception:  # pylint: disable=broad-except
            logging.warning('Dagbag loader sender errors.', exc_info=True)


def _create_dagbag(dag_folder, queue):
    """A process that creates, updates, and sync dagbag in background."""
    import airflow
    # pylint: disable=redefined-outer-name,reimported
    from airflow import configuration

    # TODO(coufon): env vars that are not exposed to subprocesses are invisible here.
    # In Composer we reload all env vars here, should check whether a similar step may
    # be needed.

    configuration = importlib.reload(configuration)
    airflow.configuration = importlib.reload(airflow.configuration)
    airflow.plugins_manager = importlib.reload(airflow.plugins_manager)
    airflow = importlib.reload(airflow)

    signal.signal(signal.SIGTERM, _kill_proc)
    signal.signal(signal.SIGINT, _kill_proc)

    logging.info('Using Asynchronous Dagbag Loader.')
    dagbag = DagBag(dag_folder, collect_dags=False)
    event_collect_done = Event()
    event_next_collect = Event()
    thread = threading.Thread(target=_send_dagbag,
                              args=(dagbag, queue, event_collect_done,
                                    event_next_collect))
    thread.daemon = True
    thread.start()
    while True:
        try:
            event_collect_done.clear()
            event_next_collect.clear()

            start_time = time.time()
            dagbag.collect_dags(dag_folder)
            event_collect_done.set()
            event_next_collect.wait()

            time.sleep(max(0, COLLECT_DAGS_INTERVAL -
                           (time.time() - start_time)))
        except SystemExit:
            sys.exit(0)
        except Exception:  # pylint: disable=broad-except
            logging.warning(
                'Dagbag loader dags collector errors.', exc_info=True)


def _cleanup_dagbag(dagbag, current_keys):
    """Delete keys in the dagbag but not in the latest collection."""
    for k in ['dags', 'file_last_changed', 'import_errors']:
        v = getattr(dagbag, k)
        for key_to_delete in set(v.keys()) - set(current_keys[k]):
            del v[key_to_delete]
        current_keys[k] = []


def _receive_dagbag(dagbag, queue):
    """A thread that receives updated dagbag."""
    current_keys = defaultdict(list)
    while True:
        try:
            collect_done, dagbag_update = queue.get()
            # Deserializing DAGs.
            if 'dags' in dagbag_update:
                dags = dagbag_update['dags']
                dagbag_update['dags'] = {
                    dag_id: deserialize(dag) for dag_id, dag in dags.items()}

            for k, v in dagbag_update.items():
                current_keys[k].extend(v.keys())
                getattr(dagbag, k).update(v)
            if collect_done:
                _cleanup_dagbag(dagbag, current_keys)
        except Exception:  # pylint: disable=broad-except
            logging.warning('Dagbag loader receiver errors.', exc_info=True)


def create_async_dagbag(dag_folder):
    """
    It creates a new process to collect DAGs with a sender thread puts updated DAGs
    on a queue. The main process creates a receiver thread to get DAGs and update
    the DagBag. All new processe and threads are daemon so the main process never
    waits for them at exiting time.
    """
    dagbag = DagBag(dag_folder, collect_dags=False)
    queue = Queue(maxsize=1)
    process = Process(target=_create_dagbag, args=(dag_folder, queue))
    process.daemon = True
    thread = threading.Thread(target=_receive_dagbag, args=(dagbag, queue))
    thread.daemon = True
    process.start()
    thread.start()
    return dagbag
