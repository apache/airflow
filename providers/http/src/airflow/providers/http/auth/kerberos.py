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

"""
Task-level Kerberos authentication utility.

This module provides task-scoped Kerberos ticket initialization for HTTP-based
tasks, enabling DAG authors to authenticate with Kerberos-protected services
without manual kinit management.
"""

from __future__ import annotations

import logging
import os
import shlex
import subprocess
import tempfile
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING

from airflow.providers.common.compat.sdk import AirflowException

if TYPE_CHECKING:
    from collections.abc import Generator

log = logging.getLogger(__name__)


class KerberosAuth:
    """
    Task-scoped Kerberos authentication manager.

    This class manages the lifecycle of Kerberos tickets for individual tasks,
    providing isolated credential caches and automatic cleanup. It is designed
    to be used with Airflow connections where keytab and principal are stored
    in the connection's extra field.

    :param principal: Kerberos principal (e.g., 'user@REALM' or 'service/host@REALM')
    :param keytab: Path to the keytab file
    :param ccache_dir: Optional directory for credential cache. If None, uses system temp.
    :param kinit_path: Path to kinit binary. Defaults to 'kinit'.
    :param forwardable: Whether to request forwardable tickets. Defaults to False.
    :param include_ip: Whether to include IP addresses in tickets. Defaults to True.

    Example:
        Using with a context manager for automatic cleanup:

        .. code-block:: python

            from airflow.providers.http.auth import KerberosAuth

            with KerberosAuth.from_connection_params(
                principal="user@REALM",
                keytab="/path/to/user.keytab"
            ) as auth:
                # Kerberos ticket is initialized and available
                # Environment variable KRB5CCNAME is set to the ticket cache
                session = requests.Session()
                session.auth = auth.get_requests_auth()
                response = session.get("https://kerberized-service.example.com/api")

            # Ticket cache is automatically cleaned up here

    Note:
        - Each instance creates an isolated credential cache
        - Tickets are automatically cleaned up when exiting the context manager
        - This does NOT modify global Kerberos state or affect webserver auth
    """

    def __init__(
        self,
        principal: str,
        keytab: str,
        ccache_dir: str | None = None,
        kinit_path: str = "kinit",
        forwardable: bool = False,
        include_ip: bool = True,
    ):
        if not principal:
            raise AirflowException("Kerberos principal is required")
        if not keytab:
            raise AirflowException("Kerberos keytab path is required")

        self.principal = principal
        self.keytab = keytab
        self.kinit_path = kinit_path
        self.forwardable = forwardable
        self.include_ip = include_ip
        self.ccache_dir = ccache_dir

        # Generate unique ccache path for this task
        self._ccache_path: str | None = None
        self._original_krb5ccname: str | None = None

    @classmethod
    def from_connection_params(
        cls,
        principal: str | None = None,
        keytab: str | None = None,
        ccache_dir: str | None = None,
        **kwargs,
    ) -> KerberosAuth:
        """
        Create KerberosAuth from connection parameters.

        :param principal: Kerberos principal
        :param keytab: Path to keytab file
        :param ccache_dir: Optional directory for credential cache
        :param kwargs: Additional arguments passed to KerberosAuth constructor
        :return: KerberosAuth instance
        """
        if not principal or not keytab:
            raise AirflowException(
                "Both 'kerberos_principal' and 'kerberos_keytab' must be provided in connection extra"
            )
        return cls(principal=principal, keytab=keytab, ccache_dir=ccache_dir, **kwargs)

    def _generate_ccache_path(self) -> str:
        """Generate a unique credential cache path for this task."""
        ccache_dir = self.ccache_dir or tempfile.gettempdir()
        unique_id = uuid.uuid4().hex
        # Use a naming pattern that clearly identifies these as task-scoped
        ccache_name = f"krb5cc_airflow_task_{unique_id}"
        return os.path.join(ccache_dir, ccache_name)

    def initialize(self) -> None:
        """
        Initialize Kerberos ticket using kinit.

        This method runs kinit to obtain a Kerberos ticket and stores it in
        a task-specific credential cache. It also sets the KRB5CCNAME
        environment variable to point to this cache.

        :raises AirflowException: If kinit fails or keytab file doesn't exist
        """
        # Verify keytab exists
        if not os.path.isfile(self.keytab):
            raise AirflowException(f"Keytab file not found: {self.keytab}")

        # Generate unique ccache path
        self._ccache_path = self._generate_ccache_path()

        # Build kinit command
        cmd = [
            self.kinit_path,
            "-f" if self.forwardable else "-F",
            "-a" if self.include_ip else "-A",
            "-k",  # Use keytab
            "-t",
            self.keytab,  # Keytab file
            "-c",
            self._ccache_path,  # Credential cache
            self.principal,
        ]

        log.info(
            "Initializing task-scoped Kerberos ticket: %s",
            " ".join(shlex.quote(str(arg)) for arg in cmd),
        )

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=False,
                timeout=30,
            )

            if result.returncode != 0:
                raise AirflowException(
                    f"kinit failed with exit code {result.returncode}.\n"
                    f"stdout: {result.stdout}\n"
                    f"stderr: {result.stderr}"
                )

            log.info("Kerberos ticket initialized successfully for principal: %s", self.principal)

            # Set KRB5CCNAME to use this ccache
            self._original_krb5ccname = os.environ.get("KRB5CCNAME")
            os.environ["KRB5CCNAME"] = self._ccache_path

        except subprocess.TimeoutExpired as e:
            raise AirflowException(f"kinit command timed out after 30 seconds: {e}")
        except FileNotFoundError:
            raise AirflowException(
                f"kinit binary not found at '{self.kinit_path}'. "
                "Ensure Kerberos client tools are installed."
            )
        except Exception as e:
            raise AirflowException(f"Failed to initialize Kerberos ticket: {e}")

    def cleanup(self) -> None:
        """
        Clean up Kerberos ticket cache.

        Removes the credential cache file and restores the original KRB5CCNAME
        environment variable.
        """
        if self._ccache_path and os.path.exists(self._ccache_path):
            try:
                os.remove(self._ccache_path)
                log.info("Removed Kerberos ticket cache: %s", self._ccache_path)
            except OSError as e:
                log.warning("Failed to remove Kerberos ticket cache %s: %s", self._ccache_path, e)

        # Restore original KRB5CCNAME
        if self._original_krb5ccname is not None:
            os.environ["KRB5CCNAME"] = self._original_krb5ccname
        elif "KRB5CCNAME" in os.environ:
            del os.environ["KRB5CCNAME"]

    @contextmanager
    def ticket(self) -> Generator[KerberosAuth, None, None]:
        """
        Context manager for task-scoped Kerberos ticket lifecycle.

        Initializes ticket on enter and cleans up on exit.

        Example:
            .. code-block:: python

                auth = KerberosAuth(principal="user@REALM", keytab="/path/to/keytab")
                with auth.ticket():
                    # Ticket is active here
                    make_authenticated_request()
                # Ticket is cleaned up here
        """
        try:
            self.initialize()
            yield self
        finally:
            self.cleanup()

    def get_requests_auth(self):
        """
        Get a requests-compatible authentication handler.

        Returns a requests_kerberos.HTTPKerberosAuth instance configured
        for the current ticket cache.

        :return: HTTPKerberosAuth instance for use with requests
        :raises ImportError: If requests-kerberos is not installed
        """
        try:
            from requests_kerberos import OPTIONAL, HTTPKerberosAuth
        except ImportError as e:
            raise ImportError(
                "requests-kerberos is required for Kerberos authentication. "
                "Install it with: pip install requests-kerberos"
            ) from e

        # HTTPKerberosAuth will use the KRB5CCNAME environment variable
        # which we set during initialize()
        return HTTPKerberosAuth(mutual_authentication=OPTIONAL)

    @property
    def ccache_path(self) -> str | None:
        """Get the path to the credential cache file."""
        return self._ccache_path
