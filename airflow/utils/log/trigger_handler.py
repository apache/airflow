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

from __future__ import annotations

import asyncio
import logging
from contextvars import ContextVar
from copy import copy
from logging.handlers import QueueHandler

from airflow.utils.log.file_task_handler import FileTaskHandler

ctx_task_instance: ContextVar = ContextVar("task_instance")
ctx_trigger_id: ContextVar = ContextVar("trigger_id")
ctx_trigger_end: ContextVar = ContextVar("trigger_end")
ctx_indiv_trigger: ContextVar = ContextVar("__individual_trigger")


class TriggerMetadataFilter(logging.Filter):
    """
    Injects TI key, triggerer job_id, and trigger_id into the log record.

    :meta private:
    """

    def filter(self, record):
        for var in (
            ctx_task_instance,
            ctx_trigger_id,
            ctx_trigger_end,
            ctx_indiv_trigger,
        ):
            val = var.get(None)
            if val is not None:
                setattr(record, var.name, val)
        return True


class DropTriggerLogsFilter(logging.Filter):
    """
    If record has attr with name ctx_indiv_trigger, filter the record.

    The purpose here is to prevent trigger logs from going to stdout
    in the trigger service.

    :meta private:
    """

    def filter(self, record):
        return getattr(record, ctx_indiv_trigger.name, None) is None


class TriggererHandlerWrapper(logging.Handler):
    """
    Wrap inheritors of FileTaskHandler and direct log messages
    to them based on trigger_id.

    :meta private:
    """

    trigger_should_queue = True

    def __init__(self, base_handler: FileTaskHandler, level=logging.NOTSET):
        super().__init__(level=level)
        self.base_handler: FileTaskHandler = base_handler
        self.handlers: dict[int, FileTaskHandler] = {}

    def _make_handler(self, ti):
        h = copy(self.base_handler)
        h.set_context(ti=ti)
        return h

    def _get_or_create_handler(self, trigger_id, ti):
        if trigger_id not in self.handlers:
            self.handlers[trigger_id] = self._make_handler(ti)
        return self.handlers[trigger_id]

    def emit(self, record):
        h = self._get_or_create_handler(record.trigger_id, record.task_instance)
        h.emit(record)

    def handle(self, record):
        if not getattr(record, ctx_indiv_trigger.name, None):
            return False
        if record.trigger_end:
            self.close_one(record.trigger_id)
            return False
        emit = self.filter(record)
        if emit:
            self.emit(record)
        return emit

    def close_one(self, trigger_id):
        h = self.handlers.get(trigger_id)
        if h:
            h.close()
            del self.handlers[trigger_id]

    def flush(self):
        for _, h in self.handlers.items():
            h.flush()

    def close(self):
        for trigger_id in list(self.handlers.keys()):
            h = self.handlers[trigger_id]
            h.close()
            del self.handlers[trigger_id]


class LocalQueueHandler(QueueHandler):
    """
    Send messages to queue.

    :meta private:
    """

    def emit(self, record: logging.LogRecord) -> None:
        # There is no need to call `prepare` because queue is in same process.
        try:
            self.enqueue(record)
        except asyncio.CancelledError:
            raise
        except Exception:
            self.handleError(record)
