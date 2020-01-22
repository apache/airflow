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

"""Kerberos command"""
import daemon
from daemon.pidfile import TimeoutPIDLockFile

from airflow import settings
from airflow.security import kerberos as krb
from airflow.utils import cli as cli_utils
from airflow.utils.cli import setup_locations


@cli_utils.action_logging
def kerberos(args):
    """Start a kerberos ticket renewer"""
    print(settings.HEADER)

    if args.daemon:
        pid, stdout, stderr, _ = setup_locations(
            "kerberos", args.pid, args.stdout, args.stderr, args.log_file
        )
        stdout = open(stdout, 'w+')
        stderr = open(stderr, 'w+')

        ctx = daemon.DaemonContext(
            pidfile=TimeoutPIDLockFile(pid, -1),
            stdout=stdout,
            stderr=stderr,
        )

        with ctx:
            krb.run(principal=args.principal, keytab=args.keytab)

        stdout.close()
        stderr.close()
    else:
        krb.run(principal=args.principal, keytab=args.keytab)
