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
#
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
"""Kerberos security provider"""
import logging
import socket
import subprocess
import sys
import time

from airflow.configuration import conf

NEED_KRB181_WORKAROUND = None

log = logging.getLogger(__name__)


def renew_from_kt(principal, keytab):
    """
    Renew kerberos token from keytab

    :param principal: principal
    :param keytab: keytab file
    :return: None
    """
    # The config is specified in seconds. But we ask for that same amount in
    # minutes to give ourselves a large renewal buffer.

    renewal_lifetime = "%sm" % conf.getint('kerberos', 'reinit_frequency')

    cmd_principal = principal or conf.get('kerberos', 'principal').replace(
        "_HOST", socket.getfqdn()
    )

    cmdv = [
        conf.get('kerberos', 'kinit_path'),
        "-r", renewal_lifetime,
        "-k",  # host ticket
        "-t", keytab,  # specify keytab
        "-c", conf.get('kerberos', 'ccache'),  # specify credentials cache
        cmd_principal
    ]
    log.info("Re-initialising kerberos from keytab: %s", " ".join(cmdv))

    subp = subprocess.Popen(cmdv,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            close_fds=True,
                            bufsize=-1,
                            universal_newlines=True)
    subp.wait()
    if subp.returncode != 0:
        log.error(
            "Couldn't reinit from keytab! `kinit' exited with %s.\n%s\n%s",
            subp.returncode,
            "\n".join(subp.stdout.readlines() if subp.stdout else []),
            "\n".join(subp.stderr.readlines() if subp.stderr else [])
        )
        sys.exit(subp.returncode)

    global NEED_KRB181_WORKAROUND
    if NEED_KRB181_WORKAROUND is None:
        NEED_KRB181_WORKAROUND = detect_conf_var()
    if NEED_KRB181_WORKAROUND:
        # (From: HUE-640). Kerberos clock have seconds level granularity. Make sure we
        # renew the ticket after the initial valid time.
        time.sleep(1.5)
        perform_krb181_workaround(principal)


def perform_krb181_workaround(principal):
    cmdv = [conf.get('kerberos', 'kinit_path'),
            "-c", conf.get('kerberos', 'ccache'),
            "-R"]  # Renew ticket_cache

    log.info(
        "Renewing kerberos ticket to work around kerberos 1.8.1: %s", " ".join(cmdv)
    )

    ret = subprocess.call(cmdv, close_fds=True)

    if ret != 0:
        principal = "%s/%s" % (principal or conf.get('kerberos', 'principal'),
                               socket.getfqdn())
        princ = principal
        ccache = conf.get('kerberos', 'principal')
        log.error(
            "Couldn't renew kerberos ticket in order to work around Kerberos 1.8.1 issue. Please check that "
            "the ticket for '%s' is still renewable:\n  $ kinit -f -c %s\nIf the 'renew until' date is the "
            "same as the 'valid starting' date, the ticket cannot be renewed. Please check your KDC "
            "configuration, and the ticket renewal policy (maxrenewlife) for the '%s' and `krbtgt' "
            "principals.", princ, ccache, princ
        )
        sys.exit(ret)


def detect_conf_var():
    """Return true if the ticket cache contains "conf" information as is found
    in ticket caches of Kerberos 1.8.1 or later. This is incompatible with the
    Sun Java Krb5LoginModule in Java6, so we need to take an action to work
    around it.
    """
    ticket_cache = conf.get('kerberos', 'ccache')

    with open(ticket_cache, 'rb') as f:
        # Note: this file is binary, so we check against a bytearray.
        return b'X-CACHECONF:' in f.read()


def run(principal, keytab):
    """
    Run the kerbros renewer.

    :param principal: principal name
    :param keytab: keytab file
    :return: None
    """
    if not keytab:
        log.debug("Keytab renewer not starting, no keytab configured")
        sys.exit(0)

    while True:
        renew_from_kt(principal, keytab)
        time.sleep(conf.getint('kerberos', 'reinit_frequency'))
