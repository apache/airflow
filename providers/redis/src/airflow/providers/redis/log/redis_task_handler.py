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
from __future__ import annotations

import logging
from functools import cached_property
from typing import TYPE_CHECKING, Any

from airflow.configuration import conf
from airflow.providers.redis.hooks.redis import RedisHook
from airflow.providers.redis.version_compat import AIRFLOW_V_3_0_PLUS
from airflow.utils.log.file_task_handler import FileTaskHandler
from airflow.utils.log.logging_mixin import LoggingMixin

if TYPE_CHECKING:
    from redis import Redis

    from airflow.models import TaskInstance


class RedisTaskHandler(FileTaskHandler, LoggingMixin):
    """
    RedisTaskHandler is a Python log handler that handles and reads task instance logs.

    It extends airflow FileTaskHandler and uploads to and reads from Redis.

    :param base_log_folder:
        base folder to store logs locally
    :param max_lines:
        Maximum number of lines of log to store
        If omitted, this is 10000.
    :param ttl_seconds:
        Maximum number of seconds to store logs
        If omitted, this is the equivalent of 28 days.
    :param conn_id:
        Airflow connection ID for the Redis hook to use
        If omitted or None, the ID specified in the option logging.remote_log_conn_id is used.
    """

    trigger_should_wrap = True

    def __init__(
        self,
        base_log_folder: str,
        max_lines: int = 10000,
        ttl_seconds: int = 60 * 60 * 24 * 28,
        conn_id: str | None = None,
    ):
        super().__init__(base_log_folder)
        self.handler: _RedisHandler | None = None
        self.max_lines = max_lines
        self.ttl_seconds = ttl_seconds
        self.conn_id = conn_id or conf.get("logging", "REMOTE_LOG_CONN_ID")

    @cached_property
    def conn(self):
        return RedisHook(redis_conn_id=self.conn_id).get_conn()

    def _read(
        self,
        ti: TaskInstance,
        try_number: int,
        metadata: dict[str, Any] | None = None,
    ):
        log_str = b"\n".join(
            self.conn.lrange(self._render_filename(ti, try_number), start=0, end=-1)
        ).decode()
        if AIRFLOW_V_3_0_PLUS:
            log_str = [log_str]  # type: ignore[assignment]
        return log_str, {"end_of_log": True}

    def set_context(self, ti: TaskInstance, **kwargs) -> None:
        super().set_context(ti)
        self.handler = _RedisHandler(
            self.conn,
            key=self._render_filename(ti, ti.try_number),
            max_lines=self.max_lines,
            ttl_seconds=self.ttl_seconds,
        )
        self.handler.setFormatter(self.formatter)


class _RedisHandler(logging.Handler):
    def __init__(self, conn: Redis, key: str, max_lines: int, ttl_seconds: int):
        super().__init__()
        self.conn = conn
        self.key = key
        self.max_lines = max_lines
        self.ttl_seconds = ttl_seconds

    def emit(self, record):
        p = self.conn.pipeline()
        p.rpush(self.key, self.format(record))
        p.ltrim(self.key, start=-self.max_lines, end=-1)
        p.expire(self.key, time=self.ttl_seconds)
        p.execute()
