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

from contextlib import contextmanager
from copy import copy
from logging import DEBUG, ERROR, INFO, WARNING
from typing import Any, Callable, Generator
from weakref import WeakKeyDictionary

from pypsrp.host import PSHost
from pypsrp.messages import MessageType
from pypsrp.powershell import PowerShell, PSInvocationState, RunspacePool
from pypsrp.wsman import WSMan

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook

INFORMATIONAL_RECORD_LEVEL_MAP = {
    MessageType.DEBUG_RECORD: DEBUG,
    MessageType.ERROR_RECORD: ERROR,
    MessageType.VERBOSE_RECORD: INFO,
    MessageType.WARNING_RECORD: WARNING,
}

OutputCallback = Callable[[str], None]


class PsrpHook(BaseHook):
    """
    Hook for PowerShell Remoting Protocol execution.

    When used as a context manager, the runspace pool is reused between shell
    sessions.

    :param psrp_conn_id: Required. The name of the PSRP connection.
    :param logging_level:
        Logging level for message streams which are received during remote execution.
        The default is to include all messages in the task log.
    :param operation_timeout: Override the default WSMan timeout when polling the pipeline.
    :param runspace_options:
        Optional dictionary which is passed when creating the runspace pool. See
        :py:class:`~pypsrp.powershell.RunspacePool` for a description of the
        available options.
    :param wsman_options:
        Optional dictionary which is passed when creating the `WSMan` client. See
        :py:class:`~pypsrp.wsman.WSMan` for a description of the available options.
    :param on_output_callback:
        Optional callback function to be called whenever an output response item is
        received during job status polling.
    :param exchange_keys:
        If true (default), automatically initiate a session key exchange when the
        hook is used as a context manager.
    :param host:
        Optional PowerShell host instance. If this is not set, the default
        implementation will be used.

    You can provide an alternative `configuration_name` using either `runspace_options`
    or by setting this key as the extra fields of your connection.
    """

    _conn = None
    _configuration_name = None
    _wsman_ref: WeakKeyDictionary[RunspacePool, WSMan] = WeakKeyDictionary()

    def __init__(
        self,
        psrp_conn_id: str,
        logging_level: int = DEBUG,
        operation_timeout: int | None = None,
        runspace_options: dict[str, Any] | None = None,
        wsman_options: dict[str, Any] | None = None,
        on_output_callback: OutputCallback | None = None,
        exchange_keys: bool = True,
        host: PSHost | None = None,
    ):
        self.conn_id = psrp_conn_id
        self._logging_level = logging_level
        self._operation_timeout = operation_timeout
        self._runspace_options = runspace_options or {}
        self._wsman_options = wsman_options or {}
        self._on_output_callback = on_output_callback
        self._exchange_keys = exchange_keys
        self._host = host or PSHost(None, None, False, type(self).__name__, None, None, "1.0")

    def __enter__(self):
        conn = self.get_conn()
        self._wsman_ref[conn].__enter__()
        conn.__enter__()
        if self._exchange_keys:
            conn.exchange_keys()
        self._conn = conn
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            self._conn.__exit__(exc_type, exc_value, traceback)
            self._wsman_ref[self._conn].__exit__(exc_type, exc_value, traceback)
        finally:
            del self._conn

    def get_conn(self) -> RunspacePool:
        """
        Returns a runspace pool.

        The returned object must be used as a context manager.
        """
        conn = self.get_connection(self.conn_id)
        self.log.info("Establishing WinRM connection %s to host: %s", self.conn_id, conn.host)

        extra = conn.extra_dejson.copy()

        def apply_extra(d, keys):
            d = d.copy()
            for key in keys:
                value = extra.pop(key, None)
                if value is not None:
                    d[key] = value
            return d

        wsman_options = apply_extra(
            self._wsman_options,
            (
                "auth",
                "cert_validation",
                "connection_timeout",
                "locale",
                "read_timeout",
                "reconnection_retries",
                "reconnection_backoff",
                "ssl",
            ),
        )
        wsman = WSMan(conn.host, username=conn.login, password=conn.password, **wsman_options)
        runspace_options = apply_extra(self._runspace_options, ("configuration_name",))

        if extra:
            raise AirflowException(f"Unexpected extra configuration keys: {', '.join(sorted(extra))}")
        pool = RunspacePool(wsman, host=self._host, **runspace_options)
        self._wsman_ref[pool] = wsman
        return pool

    @contextmanager
    def invoke(self) -> Generator[PowerShell, None, None]:
        """
        Context manager that yields a PowerShell object to which commands can be
        added. Upon exit, the commands will be invoked.
        """
        logger = copy(self.log)
        logger.setLevel(self._logging_level)
        local_context = self._conn is None
        if local_context:
            self.__enter__()
        try:
            assert self._conn is not None
            ps = PowerShell(self._conn)
            yield ps
            ps.begin_invoke()

            streams = [
                ps.output,
                ps.streams.debug,
                ps.streams.error,
                ps.streams.information,
                ps.streams.progress,
                ps.streams.verbose,
                ps.streams.warning,
            ]
            offsets = [0 for _ in streams]

            # We're using polling to make sure output and streams are
            # handled while the process is running.
            while ps.state == PSInvocationState.RUNNING:
                ps.poll_invoke(timeout=self._operation_timeout)

                for i, stream in enumerate(streams):
                    offset = offsets[i]
                    while len(stream) > offset:
                        record = stream[offset]

                        # Records received on the output stream during job
                        # status polling are handled via an optional callback,
                        # while the other streams are simply logged.
                        if stream is ps.output:
                            if self._on_output_callback is not None:
                                self._on_output_callback(record)
                        else:
                            self._log_record(logger.log, record)
                        offset += 1
                    offsets[i] = offset

            # For good measure, we'll make sure the process has
            # stopped running in any case.
            ps.end_invoke()

            self.log.info("Invocation state: %s", str(PSInvocationState(ps.state)))
            if ps.streams.error:
                raise AirflowException("Process had one or more errors")
        finally:
            if local_context:
                self.__exit__(None, None, None)

    def invoke_cmdlet(self, name: str, use_local_scope=None, **parameters: dict[str, str]) -> PowerShell:
        """Invoke a PowerShell cmdlet and return session."""
        with self.invoke() as ps:
            ps.add_cmdlet(name, use_local_scope=use_local_scope)
            ps.add_parameters(parameters)
        return ps

    def invoke_powershell(self, script: str) -> PowerShell:
        """Invoke a PowerShell script and return session."""
        with self.invoke() as ps:
            ps.add_script(script)
        return ps

    def _log_record(self, log, record):
        message_type = record.MESSAGE_TYPE
        if message_type == MessageType.ERROR_RECORD:
            log(INFO, "%s: %s", record.reason, record)
            if record.script_stacktrace:
                for trace in record.script_stacktrace.split("\r\n"):
                    log(INFO, trace)

        level = INFORMATIONAL_RECORD_LEVEL_MAP.get(message_type)
        if level is not None:
            try:
                message = str(record.message)
            except BaseException as exc:
                # See https://github.com/jborean93/pypsrp/pull/130
                message = str(exc)

            # Sometimes a message will have a trailing \r\n sequence such as
            # the tracing output of the Set-PSDebug cmdlet.
            message = message.rstrip()

            if record.command_name is None:
                log(level, "%s", message)
            else:
                log(level, "%s: %s", record.command_name, message)
        elif message_type == MessageType.INFORMATION_RECORD:
            log(INFO, "%s (%s): %s", record.computer, record.user, record.message_data)
        elif message_type == MessageType.PROGRESS_RECORD:
            log(INFO, "Progress: %s (%s)", record.activity, record.description)
        else:
            log(WARNING, "Unsupported message type: %s", message_type)
