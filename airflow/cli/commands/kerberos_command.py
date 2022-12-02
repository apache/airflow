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
"""Kerberos command."""
from __future__ import annotations

import daemon
from daemon.pidfile import TimeoutPIDLockFile

from airflow import settings
from airflow.security import kerberos as krb
from airflow.utils import cli as cli_utils
from airflow.utils.cli import setup_locations


@cli_utils.action_cli
def kerberos(args):
    """Start a kerberos ticket renewer."""
    print(settings.HEADER)

    if args.daemon:
        pid, stdout, stderr, _ = setup_locations(
            "kerberos", args.pid, args.stdout, args.stderr, args.log_file
        )
        with open(stdout, "a") as stdout_handle, open(stderr, "a") as stderr_handle:
            stdout_handle.truncate(0)
            stderr_handle.truncate(0)

            ctx = daemon.DaemonContext(
                pidfile=TimeoutPIDLockFile(pid, -1),
                stdout=stdout_handle,
                stderr=stderr_handle,
                umask=int(settings.DAEMON_UMASK, 8),
            )

            with ctx:
                krb.run(principal=args.principal, keytab=args.keytab)
    else:
        krb.run(principal=args.principal, keytab=args.keytab)
