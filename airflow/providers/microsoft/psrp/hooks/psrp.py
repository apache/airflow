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

from time import sleep

from pypsrp.messages import ErrorRecord, InformationRecord, ProgressRecord
from pypsrp.powershell import PowerShell, PSInvocationState, RunspacePool
from pypsrp.wsman import WSMan

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook


class PSRPHook(BaseHook):
    """
    Hook for PowerShell Remoting Protocol execution.

    The hook must be used as a context manager.
    """

    _client = None
    _poll_interval = 1

    def __init__(self, psrp_conn_id: str):
        self.conn_id = psrp_conn_id

    def __enter__(self):
        conn = self.get_connection(self.conn_id)

        self.log.info("Establishing WinRM connection %s to host: %s", self.conn_id, conn.host)
        self._client = WSMan(
            conn.host,
            ssl=True,
            auth="ntlm",
            encryption="never",
            username=conn.login,
            password=conn.password,
            cert_validation=False,
        )
        self._client.__enter__()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            self._client.__exit__(exc_type, exc_value, traceback)
        finally:
            self._client = None

    def invoke_powershell(self, script: str) -> PowerShell:
        with RunspacePool(self._client) as pool:
            ps = PowerShell(pool)
            ps.add_script(script)
            ps.begin_invoke()
            streams = [
                (ps.output, self._log_output),
                (ps.streams.debug, self._log_record),
                (ps.streams.information, self._log_record),
                (ps.streams.error, self._log_record),
            ]
            offsets = [0 for _ in streams]

            # We're using polling to make sure output and streams are
            # handled while the process is running.
            while ps.state == PSInvocationState.RUNNING:
                sleep(self._poll_interval)
                ps.poll_invoke()

                for (i, (stream, handler)) in enumerate(streams):
                    offset = offsets[i]
                    while len(stream) > offset:
                        handler(stream[offset])
                        offset += 1
                    offsets[i] = offset

            # For good measure, we'll make sure the process has
            # stopped running.
            ps.end_invoke()

            if ps.streams.error:
                raise AirflowException("Process had one or more errors")

            self.log.info("Invocation state: %s", str(PSInvocationState(ps.state)))
            return ps

    def _log_output(self, message: str):
        self.log.info("%s", message)

    def _log_record(self, record):
        # TODO: Consider translating some or all of these records into
        # normal logging levels, using `log(level, msg, *args)`.
        if isinstance(record, ErrorRecord):
            self.log.info("Error: %s", record)
            return

        if isinstance(record, InformationRecord):
            self.log.info("Information: %s", record.message_data)
            return

        if isinstance(record, ProgressRecord):
            self.log.info("Progress: %s (%s)", record.activity, record.description)
            return

        self.log.info("Unsupported record type: %s", type(record).__name__)
