#!/usr/bin/env python
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

from enum import Enum

# Licensed to Cloudera, Inc. under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  Cloudera, Inc. licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Kerberos security provider."""
import logging
import shlex
import subprocess
import sys
import time
from functools import cache

from airflow.configuration import conf
from airflow.utils.net import get_hostname

log = logging.getLogger(__name__)


class KerberosMode(Enum):
    """
    Defines modes for running airflow kerberos.

    :return: None.
    """

    STANDARD = "standard"
    ONE_TIME = "one-time"


def get_kerberos_principal(principal: str | None) -> str:
    """Retrieve Kerberos principal. Fallback to principal from Airflow configuration if not provided."""
    return principal or conf.get_mandatory_value("kerberos", "principal").replace("_HOST", get_hostname())


def parse_kinit_args(
    principal: str | None = None,
    service: str | None = None,
    keytab: str | None = None,
    renewal_lifetime: int | None = None,
    forwardable: bool | None = None,
    include_ip: bool | None = None,
    cache: str | None = None,
):
    """Parses kinit args into a command string.

    :param principal: Principal, optional.
    :param service: Service, optional.
    :param keytab: Keytab file, optional. If keytab is omitted, kinit will prompt interactively for a password,
        blocking indefinitely
    :param renewal_lifetime: Ticket renewal lifetime, defaults to `[kerberos] reinit_frequency`
    setting.
    :param forwardable: Is ticket forwardable, defaults to `[kerberos] forwardable` setting.
    :param include_ip: Include IP, defaults to `[kerberos] include_ip` setting.
    :param cache: Kerberos cache, defaults to `[kerberos] ccache` setting.
    :return: Parsed command args as a list.
    """
    # Get default options from config
    if renewal_lifetime is None:
        renewal_lifetime = conf.getint("kerberos", "reinit_frequency")

    if forwardable is None:
        forwardable = conf.getboolean("kerberos", "forwardable")

    if include_ip is None:
        include_ip = conf.getboolean("kerberos", "include_ip")

    if cache is None:
        cache = conf.get_mandatory_value("kerberos", "ccache")

    # The config is specified in seconds. But we ask for that same amount in
    # minutes to give ourselves a large renewal buffer.
    renewal_lifetime_arg = f"{renewal_lifetime}m"
    if forwardable:
        forwardable_arg = "-f"
    else:
        forwardable_arg = "-F"
    if include_ip:
        include_ip_arg = "-a"
    else:
        include_ip_arg = "-A"
    if keytab:
        keytab_args = ["-k", "-t", keytab]
    else:  # Omit keytab for password auth
        keytab_args = []
    if service:
        service_args = ["-S", service]
    else:
        service_args = []
    if principal is not None:
        cmd_principal = [principal]
    else:
        cmd_principal = []

    return (
        [
            conf.get_mandatory_value("kerberos", "kinit_path"),
            forwardable_arg,
            include_ip_arg,
            "-r",
            renewal_lifetime_arg,
        ]
        + keytab_args
        + ["-c", cache]
        + service_args
        + cmd_principal
    )


def renew_ticket(
    principal: str | None,
    keytab: str | None = None,
    password: str | None = None,
    exit_on_fail: bool = True,
    **kinit_args,
):
    """Renew the kerberos ticket for a given principal using a keytab or password.
    Additional args are passed to `parse_kinit_args` and converted to `kinit` options.

    :param principal: Kerberos principal.
    :param keytab: Keytab file, optional,
        must be provided if password is omitted.
    :param password: Password, optional,
        must be provided if keytab is omitted.
    :param service: Service, optional.
    :param forwardable: is ticket forwardable, optional.
    :param include_ip: include ip address in ticket, optional.
    :param cache: kerberos ccache to use, optional.
    :param exit_on_fail: Propagate errors from subprocess, defaults to True
    :raises: ValueError if no keytab or password is provided
    :return: kinit return code
    """
    if keytab is None and password is None:
        raise ValueError("`kinit` requires `keytab` or `password` for authentication")
    cmd_principal = get_kerberos_principal(principal)

    cmdv = parse_kinit_args(
        cmd_principal,
        keytab=keytab,
        **kinit_args,
    )

    log.info("Re-initialising kerberos ticket: %s", " ".join(shlex.quote(f) for f in cmdv))
    with subprocess.Popen(
        cmdv,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        stdin=subprocess.PIPE,  # Required to answer password prompt
        close_fds=True,
        bufsize=-1,
        universal_newlines=True,
    ) as subp:
        if keytab is None and password is not None:  # Answer password prompt if no keytab
            subp.communicate(password.strip().encode())
        subp.wait()
        if subp.returncode != 0:
            log.error(
                "Couldn't reinit ticket! `kinit` exited with %s.\n%s\n%s",
                subp.returncode,
                "\n".join(subp.stdout.readlines() if subp.stdout else []),
                "\n".join(subp.stderr.readlines() if subp.stderr else []),
            )
            if exit_on_fail:
                sys.exit(subp.returncode)
            else:
                return subp.returncode

    if detect_conf_var():
        # (From: HUE-640). Kerberos clock have seconds level granularity. Make sure we
        # renew the ticket after the initial valid time.
        time.sleep(1.5)
        ret = perform_krb181_workaround(cmd_principal)
        if exit_on_fail and ret != 0:
            sys.exit(ret)
        else:
            return ret
    return 0


def renew_from_kt(principal: str | None, keytab: str, exit_on_fail: bool = True):
    """
    Renew kerberos token from keytab.

    :param principal: principal
    :param keytab: keytab file
    :return: kinit return code
    """
    return renew_ticket(
        principal=principal,
        keytab=keytab,
        exit_on_fail=exit_on_fail,
    )


def perform_krb181_workaround(principal: str):
    """
    Workaround for Kerberos 1.8.1.

    :param principal: principal name
    :return: None
    """
    cmdv: list[str] = [
        conf.get_mandatory_value("kerberos", "kinit_path"),
        "-c",
        conf.get_mandatory_value("kerberos", "ccache"),
        "-R",
    ]  # Renew ticket_cache

    log.info("Renewing kerberos ticket to work around kerberos 1.8.1: %s", " ".join(cmdv))

    ret = subprocess.call(cmdv, close_fds=True)

    if ret != 0:
        principal = f"{principal or conf.get('kerberos', 'principal')}/{get_hostname()}"
        ccache = conf.get("kerberos", "ccache")
        log.error(
            "Couldn't renew kerberos ticket in order to work around Kerberos 1.8.1 issue. Please check that "
            "the ticket for '%s' is still renewable:\n  $ kinit -f -c %s\nIf the 'renew until' date is the "
            "same as the 'valid starting' date, the ticket cannot be renewed. Please check your KDC "
            "configuration, and the ticket renewal policy (maxrenewlife) for the '%s' and `krbtgt' "
            "principals.",
            principal,
            ccache,
            principal,
        )
    return ret


@cache
def detect_conf_var() -> bool:
    """
    Autodetect the Kerberos ticket configuration.

    Return true if the ticket cache contains "conf" information as is found
    in ticket caches of Kerberos 1.8.1 or later. This is incompatible with the
    Sun Java Krb5LoginModule in Java6, so we need to take an action to work
    around it.
    """
    ticket_cache = conf.get_mandatory_value("kerberos", "ccache")

    with open(ticket_cache, "rb") as file:
        # Note: this file is binary, so we check against a bytearray.
        return b"X-CACHECONF:" in file.read()


def run(principal: str | None, keytab: str, mode: KerberosMode = KerberosMode.STANDARD):
    """
    Run the kerberos renewer.

    :param principal: principal name
    :param keytab: keytab file
    :param mode: mode to run the airflow kerberos in
    :return: None
    """
    if not keytab:
        log.warning("Keytab renewer not starting, no keytab configured")
        sys.exit(0)

    log.info("Using airflow kerberos with mode: %s", mode.value)

    if mode == KerberosMode.STANDARD:
        while True:
            renew_from_kt(principal, keytab)
            time.sleep(conf.getint("kerberos", "reinit_frequency"))
    elif mode == KerberosMode.ONE_TIME:
        renew_from_kt(principal, keytab)
