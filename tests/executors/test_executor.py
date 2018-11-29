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
from airflow.executors.base_executor import BaseExecutor
from airflow.utils.state import State

from airflow import settings


class TestExecutor(BaseExecutor):
    """
    TestExecutor is used for unit testing purposes.
    """

    def __init__(self, do_update=False, *args, **kwargs):
        self.do_update = do_update
        self._running = []
        self.history = []

        super(TestExecutor, self).__init__(*args, **kwargs)

    def execute_async(self, key, command, queue=None):
        self.log.debug("{} running task instances".format(len(self.running)))
        self.log.debug("{} in queue".format(len(self.queued_tasks)))

    def heartbeat(self):
        session = settings.Session()
        if self.do_update:
            self.history.append(list(self.queued_tasks.values()))
            while len(self._running) > 0:
                ti = self._running.pop()
                ti.set_state(State.SUCCESS, session)
            for key, val in list(self.queued_tasks.items()):
                (command, priority, queue, simple_ti) = val
                ti = simple_ti.construct_task_instance()
                ti.set_state(State.RUNNING, session)
                self._running.append(ti)
                self.queued_tasks.pop(key)

        session.commit()
        session.close()

    def terminate(self):
        pass

    def end(self):
        self.sync()
