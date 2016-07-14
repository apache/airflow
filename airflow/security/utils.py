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
#

import re
import socket
import airflow.configuration as conf

# Pattern to replace with hostname
HOSTNAME_PATTERN = '_HOST'


def get_kerberos_principal(principal, host):
    components = get_components(principal)
    if not components or len(components) != 3 or components[1] != HOSTNAME_PATTERN:
        return principal
    else:
        if not host:
            raise IOError("Can't replace %s pattern since host is null." % HOSTNAME_PATTERN)
        return replace_hostname_pattern(components, host)


def get_components(principal):
    """
    get_components(principal) -> (short name, instance (FQDN), realm)

    ``principal`` is the kerberos principal to parse.
    """
    if not principal:
        return None
    return re.split('[\/@]', str(principal))


def replace_hostname_pattern(components, host=None):
    fqdn = host
    if not fqdn or fqdn == '0.0.0.0':
        fqdn = get_localhost_name()
    return '%s/%s@%s' % (components[0], fqdn.lower(), components[2])


def get_localhost_name():
    return socket.getfqdn()


def get_fqdn(hostname_or_ip=None):
    # Get hostname
    try:
        if hostname_or_ip:
            fqdn = socket.gethostbyaddr(hostname_or_ip)[0]
        else:
            fqdn = get_localhost_name()
    except IOError:
        fqdn = hostname_or_ip

    if fqdn == 'localhost':
        fqdn = get_localhost_name()

    return fqdn


def principal_from_username(username):
    realm = conf.get("security", "default_realm")
    if '@' not in username and realm:
        username = "{}@{}".format(username, realm)

    return username
