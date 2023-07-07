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
"""Various security-related utils."""
import socket

import re2

from airflow.utils.net import get_hostname


def get_components(principal) -> list[str] | None:
    """Split the kerberos principal string into parts.

    :return: *None* if the principal is empty. Otherwise split the value into
        parts. Assuming the principal string is valid, the return value should
        contain three components: short name, instance (FQDN), and realm.
    """
    if not principal:
        return None
    return re2.split(r"[/@]", str(principal))


def replace_hostname_pattern(components, host=None):
    """Replaces hostname with the right pattern including lowercase of the name."""
    fqdn = host
    if not fqdn or fqdn == "0.0.0.0":
        fqdn = get_hostname()
    return f"{components[0]}/{fqdn.lower()}@{components[2]}"


def get_fqdn(hostname_or_ip=None):
    """Retrieves FQDN - hostname for the IP or hostname."""
    try:
        if hostname_or_ip:
            fqdn = socket.gethostbyaddr(hostname_or_ip)[0]
            if fqdn == "localhost":
                fqdn = get_hostname()
        else:
            fqdn = get_hostname()
    except OSError:
        fqdn = hostname_or_ip

    return fqdn


def principal_from_username(username, realm):
    """Retrieves principal from the user name and realm."""
    if ("@" not in username) and realm:
        username = f"{username}@{realm}"

    return username
