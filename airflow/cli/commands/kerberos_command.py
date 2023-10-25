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

from airflow import settings
from airflow.cli.commands.daemon_utils import run_command_with_daemon_option
from airflow.security import kerberos as krb
from airflow.security.kerberos import KerberosMode
from airflow.utils import cli as cli_utils
from airflow.utils.providers_configuration_loader import providers_configuration_loaded


@cli_utils.action_cli
@providers_configuration_loaded
def kerberos(args):
    """Start a kerberos ticket renewer."""
    print(settings.HEADER)

    mode_mapping = {
        "daemon": KerberosMode.DAEMON,
        "one-time": KerberosMode.ONE_TIME,
    }
    if args.mode:
        mode_enum = mode_mapping.get(args.mode)
        if mode_enum is None:
            raise ValueError("Invalid mode. Mode must be 'daemon' or 'one-time'.")
    else:
        mode_enum = KerberosMode.DAEMON

    run_command_with_daemon_option(
        args=args,
        process_name="kerberos",
        callback=lambda: krb.run(principal=args.principal, keytab=args.keytab, mode=mode_enum),
    )
