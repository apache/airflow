#!/usr/bin/env python
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

import logging
import subprocess
import sys
import time
import socket

from airflow import configuration

LOG = logging.getLogger(__name__)

NEED_KRB181_WORKAROUND=None


def renew_from_kt():
    # The config is specified in seconds. But we ask for that same amount in
    # minutes to give ourselves a large renewal buffer.
    renewal_lifetime = "%sm" % configuration.getint('kerberos', 'reinit_frequency')
    principal = configuration.get('kerberos', 'principal').replace("_HOST", socket.getfqdn())
    cmdv = [configuration.get('kerberos', 'kinit_path'),
            "-r", renewal_lifetime,
            "-k",  # host ticket
            "-t", configuration.get('kerberos', 'keytab'),   # specify keytab
            "-c", configuration.get('kerberos', 'ccache'),   # specify credentials cache
            principal]
    LOG.info("Reinitting kerberos from keytab: " +
             " ".join(cmdv))

    subp = subprocess.Popen(cmdv,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            close_fds=True,
                            bufsize=-1)
    subp.wait()
    if subp.returncode != 0:
        LOG.error("Couldn't reinit from keytab! `kinit' exited with %s.\n%s\n%s" % (
            subp.returncode,
            "\n".join(subp.stdout.readlines()),
            "\n".join(subp.stderr.readlines())))
        sys.exit(subp.returncode)

    global NEED_KRB181_WORKAROUND
    if NEED_KRB181_WORKAROUND is None:
        NEED_KRB181_WORKAROUND = detect_conf_var()
    if NEED_KRB181_WORKAROUND:
        # (From: HUE-640). Kerberos clock have seconds level granularity. Make sure we
        # renew the ticket after the initial valid time.
        time.sleep(1.5)
        perform_krb181_workaround()


def perform_krb181_workaround():
    cmdv = [configuration.get('kerberos', 'kinit_path'),
            "-R",
            "-c", configuration.get('kerberos', 'ccache')]
    LOG.info("Renewing kerberos ticket to work around kerberos 1.8.1: " +
             " ".join(cmdv))
    ret = subprocess.call(cmdv)
    if ret != 0:
        principal = "%s/%s" % (configuration.get('kerberos', 'principal'), socket.getfqdn())
        fmt_dict = dict(princ=principal,
                        ccache=configuration.get('kerberos', 'principal'))
        LOG.error("Couldn't renew kerberos ticket in order to work around "
                  "Kerberos 1.8.1 issue. Please check that the ticket for "
                  "'%(princ)s' is still renewable:\n"
                  "  $ kinit -f -c %(ccache)s\n"
                  "If the 'renew until' date is the same as the 'valid starting' "
                  "date, the ticket cannot be renewed. Please check your KDC "
                  "configuration, and the ticket renewal policy (maxrenewlife) "
                  "for the '%(princ)s' and `krbtgt' principals." % fmt_dict)
        sys.exit(ret)


def detect_conf_var():
    """Return true if the ticket cache contains "conf" information as is found
    in ticket caches of Kerboers 1.8.1 or later. This is incompatible with the
    Sun Java Krb5LoginModule in Java6, so we need to take an action to work
    around it.
    """
    with open(configuration.get('kerberos', 'ccache'), "rb") as f:
        return "X-CACHECONF:" in f.read()


def run():
    if configuration.get('kerberos','keytab') is None:
        LOG.debug("Keytab renewer not starting, no keytab configured")
        sys.exit(0)

    while True:
        renew_from_kt()
        time.sleep(configuration.getint('kerberos', 'reinit_frequency'))
