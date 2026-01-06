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
"""Hook for winrm remote execution."""

from __future__ import annotations

from base64 import b64encode
from contextlib import suppress

from winrm.exceptions import WinRMOperationTimeoutError
from winrm.protocol import Protocol

from airflow.providers.common.compat.sdk import AirflowException, BaseHook
from airflow.utils.platform import getuser

# TODO: FIXME please - I have too complex implementation


class WinRMHook(BaseHook):
    """
    Hook for winrm remote execution using pywinrm.

    :seealso: https://github.com/diyan/pywinrm/blob/master/winrm/protocol.py

    :param ssh_conn_id: connection id from airflow Connections from where
        all the required parameters can be fetched like username and password,
        though priority is given to the params passed during init.
    :param endpoint: When not set, endpoint will be constructed like this:
        'http://{remote_host}:{remote_port}/wsman'
    :param remote_host: Remote host to connect to. Ignored if `endpoint` is set.
    :param remote_port: Remote port to connect to. Ignored if `endpoint` is set.
    :param transport: transport type, one of 'plaintext' (default), 'kerberos', 'ssl', 'ntlm', 'credssp'
    :param username: username to connect to the remote_host
    :param password: password of the username to connect to the remote_host
    :param service: the service name, default is HTTP
    :param keytab: the path to a keytab file if you are using one
    :param ca_trust_path: Certification Authority trust path
    :param cert_pem: client authentication certificate file path in PEM format
    :param cert_key_pem: client authentication certificate key file path in PEM format
    :param server_cert_validation: whether server certificate should be validated on
        Python versions that support it; one of 'validate' (default), 'ignore'
    :param kerberos_delegation: if True, TGT is sent to target server to
        allow multiple hops
    :param read_timeout_sec: maximum seconds to wait before an HTTP connect/read times out (default 30).
        This value should be slightly higher than operation_timeout_sec,
        as the server can block *at least* that long.
    :param operation_timeout_sec: maximum allowed time in seconds for any single wsman
        HTTP operation (default 20). Note that operation timeouts while receiving output
        (the only wsman operation that should take any significant time,
        and where these timeouts are expected) will be silently retried indefinitely.
    :param kerberos_hostname_override: the hostname to use for the kerberos exchange
        (defaults to the hostname in the endpoint URL)
    :param message_encryption: Will encrypt the WinRM messages if set
        and the transport auth supports message encryption. (Default 'auto')
    :param credssp_disable_tlsv1_2: Whether to disable TLSv1.2 support and work with older
        protocols like TLSv1.0, default is False
    :param send_cbt: Will send the channel bindings over a HTTPS channel (Default: True)
    """

    conn_name_attr = "ssh_conn_id"
    default_conn_name = "winrm_default"
    conn_type = "winrm"
    hook_name = "WinRM"

    def __init__(
        self,
        ssh_conn_id: str | None = None,
        endpoint: str | None = None,
        remote_host: str | None = None,
        remote_port: int = 5985,
        transport: str = "plaintext",
        username: str | None = None,
        password: str | None = None,
        service: str = "HTTP",
        keytab: str | None = None,
        ca_trust_path: str | None = None,
        cert_pem: str | None = None,
        cert_key_pem: str | None = None,
        server_cert_validation: str = "validate",
        kerberos_delegation: bool = False,
        read_timeout_sec: int = 30,
        operation_timeout_sec: int = 20,
        kerberos_hostname_override: str | None = None,
        message_encryption: str | None = "auto",
        credssp_disable_tlsv1_2: bool = False,
        send_cbt: bool = True,
    ) -> None:
        super().__init__()
        self.ssh_conn_id = ssh_conn_id
        self.endpoint = endpoint
        self.remote_host = remote_host
        self.remote_port = remote_port
        self.transport = transport
        self.username = username
        self.password = password
        self.service = service
        self.keytab = keytab
        self.ca_trust_path = ca_trust_path
        self.cert_pem = cert_pem
        self.cert_key_pem = cert_key_pem
        self.server_cert_validation = server_cert_validation
        self.kerberos_delegation = kerberos_delegation
        self.read_timeout_sec = read_timeout_sec
        self.operation_timeout_sec = operation_timeout_sec
        self.kerberos_hostname_override = kerberos_hostname_override
        self.message_encryption = message_encryption
        self.credssp_disable_tlsv1_2 = credssp_disable_tlsv1_2
        self.send_cbt = send_cbt

        self.winrm_protocol = None

    def get_conn(self):
        self.log.debug("Creating WinRM client for conn_id: %s", self.ssh_conn_id)
        if self.ssh_conn_id is not None:
            conn = self.get_connection(self.ssh_conn_id)

            if self.username is None:
                self.username = conn.login
            if self.password is None:
                self.password = conn.password
            if self.remote_host is None:
                self.remote_host = conn.host

            if conn.extra is not None:
                extra_options = conn.extra_dejson

                if "endpoint" in extra_options:
                    self.endpoint = str(extra_options["endpoint"])
                if "remote_port" in extra_options:
                    self.remote_port = int(extra_options["remote_port"])
                if "transport" in extra_options:
                    self.transport = str(extra_options["transport"])
                if "service" in extra_options:
                    self.service = str(extra_options["service"])
                if "keytab" in extra_options:
                    self.keytab = str(extra_options["keytab"])
                if "ca_trust_path" in extra_options:
                    self.ca_trust_path = str(extra_options["ca_trust_path"])
                if "cert_pem" in extra_options:
                    self.cert_pem = str(extra_options["cert_pem"])
                if "cert_key_pem" in extra_options:
                    self.cert_key_pem = str(extra_options["cert_key_pem"])
                if "server_cert_validation" in extra_options:
                    self.server_cert_validation = str(extra_options["server_cert_validation"])
                if "kerberos_delegation" in extra_options:
                    self.kerberos_delegation = str(extra_options["kerberos_delegation"]).lower() == "true"
                if "read_timeout_sec" in extra_options:
                    self.read_timeout_sec = int(extra_options["read_timeout_sec"])
                if "operation_timeout_sec" in extra_options:
                    self.operation_timeout_sec = int(extra_options["operation_timeout_sec"])
                if "kerberos_hostname_override" in extra_options:
                    self.kerberos_hostname_override = str(extra_options["kerberos_hostname_override"])
                if "message_encryption" in extra_options:
                    self.message_encryption = str(extra_options["message_encryption"])
                if "credssp_disable_tlsv1_2" in extra_options:
                    self.credssp_disable_tlsv1_2 = (
                        str(extra_options["credssp_disable_tlsv1_2"]).lower() == "true"
                    )
                if "send_cbt" in extra_options:
                    self.send_cbt = str(extra_options["send_cbt"]).lower() == "true"

        if not self.remote_host:
            raise AirflowException("Missing required param: remote_host")

        # Auto detecting username values from system
        if not self.username:
            self.log.debug(
                "username to WinRM to host: %s is not specified for connection id"
                " %s. Using system's default provided by getpass.getuser()",
                self.remote_host,
                self.ssh_conn_id,
            )
            self.username = getuser()

        # If endpoint is not set, then build a standard wsman endpoint from host and port.
        if not self.endpoint:
            self.endpoint = f"http://{self.remote_host}:{self.remote_port}/wsman"

        try:
            if self.password and self.password.strip():
                self.winrm_protocol = Protocol(
                    endpoint=self.endpoint,
                    transport=self.transport,
                    username=self.username,
                    password=self.password,
                    service=self.service,
                    keytab=self.keytab,
                    ca_trust_path=self.ca_trust_path,
                    cert_pem=self.cert_pem,
                    cert_key_pem=self.cert_key_pem,
                    server_cert_validation=self.server_cert_validation,
                    kerberos_delegation=self.kerberos_delegation,
                    read_timeout_sec=self.read_timeout_sec,
                    operation_timeout_sec=self.operation_timeout_sec,
                    kerberos_hostname_override=self.kerberos_hostname_override,
                    message_encryption=self.message_encryption,
                    credssp_disable_tlsv1_2=self.credssp_disable_tlsv1_2,
                    send_cbt=self.send_cbt,
                )

        except Exception as error:
            error_msg = f"Error creating connection to host: {self.remote_host}, error: {error}"
            self.log.error(error_msg)
            raise AirflowException(error_msg)

        if not hasattr(self.winrm_protocol, "get_command_output_raw"):
            # since pywinrm>=0.5 get_command_output_raw replace _raw_get_command_output
            self.winrm_protocol.get_command_output_raw = self.winrm_protocol._raw_get_command_output

        return self.winrm_protocol

    def run(
        self,
        command: str,
        ps_path: str | None = None,
        output_encoding: str = "utf-8",
        return_output: bool = True,
        working_directory: str | None = None,
    ) -> tuple[int, list[bytes], list[bytes]]:
        """
        Run a command.

        :param command: command to execute on remote host.
        :param ps_path: path to powershell, `powershell` for v5.1- and `pwsh` for v6+.
            If specified, it will execute the command as powershell script.
        :param output_encoding: the encoding used to decode stout and stderr.
        :param return_output: Whether to accumulate and return the stdout or not.
        :param working_directory: specify working directory.
        :return: returns a tuple containing return_code, stdout and stderr in order.
        """
        winrm_client = self.get_conn()
        self.log.info("Establishing WinRM connection to host: %s", self.remote_host)
        try:
            shell_id = winrm_client.open_shell(working_directory=working_directory)
        except Exception as error:
            error_msg = f"Error connecting to host: {self.remote_host}, error: {error}"
            self.log.error(error_msg)
            raise AirflowException(error_msg)

        try:
            if ps_path is not None:
                self.log.info("Running command as powershell script: '%s'...", command)
                encoded_ps = b64encode(command.encode("utf_16_le")).decode("ascii")
                command_id = winrm_client.run_command(shell_id, f"{ps_path} -encodedcommand {encoded_ps}")
            else:
                self.log.info("Running command: '%s'...", command)
                command_id = winrm_client.run_command(shell_id, command)

                # See: https://github.com/diyan/pywinrm/blob/master/winrm/protocol.py
            stdout_buffer = []
            stderr_buffer = []
            command_done = False
            while not command_done:
                # this is an expected error when waiting for a long-running process, just silently retry
                with suppress(WinRMOperationTimeoutError):
                    (
                        stdout,
                        stderr,
                        return_code,
                        command_done,
                    ) = winrm_client.get_command_output_raw(shell_id, command_id)

                    # Only buffer stdout if we need to so that we minimize memory usage.
                    if return_output:
                        stdout_buffer.append(stdout)
                    stderr_buffer.append(stderr)

                    for line in stdout.decode(output_encoding).splitlines():
                        self.log.info(line)
                    for line in stderr.decode(output_encoding).splitlines():
                        self.log.warning(line)

            winrm_client.cleanup_command(shell_id, command_id)

            return return_code, stdout_buffer, stderr_buffer
        except Exception as e:
            raise AirflowException(f"WinRM operator error: {e}")
        finally:
            winrm_client.close_shell(shell_id)

    def test_connection(self):
        try:
            (r_code, std_out, std_err) = self.run("cd")
            if r_code != 0:
                raise RuntimeError(std_err)
            return True, "Connection successful."
        except Exception as e:
            return False, str(e)
