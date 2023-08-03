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
from __future__ import annotations

import socket
from functools import lru_cache
from urllib.parse import urljoin

from airflow.configuration import conf


# patched version of socket.getfqdn() - see https://github.com/python/cpython/issues/49254
@lru_cache(maxsize=None)
def getfqdn(name=""):
    """
    Get fully qualified domain name from name.

    An empty argument is interpreted as meaning the local host.
    """
    name = name.strip()
    if not name or name == "0.0.0.0":
        name = socket.gethostname()
    try:
        addrs = socket.getaddrinfo(name, None, 0, socket.SOCK_DGRAM, 0, socket.AI_CANONNAME)
    except OSError:
        pass
    else:
        for addr in addrs:
            if addr[3]:
                name = addr[3]
                break
    return name


def get_host_ip_address():
    """Fetch host ip address."""
    return socket.gethostbyname(getfqdn())


def get_hostname():
    """Fetch the hostname using the callable from config or use `airflow.utils.net.getfqdn` as a fallback."""
    return conf.getimport("core", "hostname_callable", fallback="airflow.utils.net.getfqdn")()


def safe_urljoin(base_url: str, relative_url: str, **kwargs) -> str:
    """Safely join the given URLs."""
    # Since 'urljoin' strips everything after the last trailing slash, the last part of the path in the
    # base URL would be skipped if it does not end with a slash. Hence, we ensure here and update the
    # base_url so that it has a trailing slash.
    if not base_url.endswith("/"):
        base_url += "/"

    # Ensure relative_url does not have a leading slash and if it has one remove it. This is because if
    # the relative URL has a leading slash, urljoin skips the base URL altogether and just returns the
    # relative URL as the output.
    if relative_url.startswith("/"):
        relative_url = relative_url[1:]

    return urljoin(base_url, relative_url, **kwargs)
