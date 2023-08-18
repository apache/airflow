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
from urllib.parse import urlsplit, urlunsplit

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


def replace_items_from_url(value, function) -> str:
    if not value:
        return value

    url_parts = urlsplit(value)
    netloc = None
    if url_parts.netloc:
        # unpack
        userinfo = None
        username = None
        password = None

        if "@" in url_parts.netloc:
            userinfo, _, host = url_parts.netloc.partition("@")
        else:
            host = url_parts.netloc
        if userinfo:
            if ":" in userinfo:
                username, _, password = userinfo.partition(":")
            else:
                username = userinfo

        username, password = function(username, password)

        # pack
        if username and password and host:
            netloc = username + ":" + password + "@" + host
        elif username and host:
            netloc = username + "@" + host
        elif password and host:
            netloc = ":" + password + "@" + host
        elif host:
            netloc = host
        else:
            netloc = ""

    return urlunsplit((url_parts.scheme, netloc, url_parts.path, url_parts.query, url_parts.fragment))


def replace_password_from_url(value, replace_value) -> str:
    return replace_items_from_url(value, lambda username, password: (username, replace_value))
