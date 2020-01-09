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

import contextlib
import importlib

from airflow import configuration


@contextlib.contextmanager
def acquire_lock(callback=None):
    """
    A context manager that acquires a lock to run the scheduler
    If LOCK_BACKEND is not configured, this is a no-op context manager.
    If LOCK_BACKEND is set, it should be a module with an acquire_lock function defined.

    :param callback: A function to call if the lock has been stolen.
    :type callback: func
    :return: A context manager with which to acquire the lock, or an exception if lock cannot be acquired.
    """
    lock_backend = None if not configuration.conf.has_option('scheduler', 'LOCK_BACKEND') \
        else configuration.conf.get('scheduler', 'LOCK_BACKEND', fallback=None)
    if not lock_backend:
        yield
    else:
        module = importlib.import_module(lock_backend)
        backend = getattr(module, 'acquire_lock')
        with backend(callback=callback):
            yield


def initialize():
    """
    Initialization for the lock backend.
    Used if the lock requires a database or other resources to be created.
    If LOCK_BACKEND is not configured, this is a no-op.
    If LOCK_BACKEND is set, it should be a module with an initialize function defined.
    """
    lock_backend = None if not configuration.conf.has_option('scheduler', 'LOCK_BACKEND') \
        else configuration.conf.get('scheduler', 'LOCK_BACKEND', fallback=None)
    if not lock_backend:
        return
    module = importlib.import_module(lock_backend)
    backend_init = getattr(module, 'initialize')
    backend_init()
