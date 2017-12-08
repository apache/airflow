# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from airflow.configuration import (conf, AirflowConfigException)
import socket


_sentinel = object()


def get_hostname(default=socket.getfqdn):
    """
    A replacement for `socket.getfqdn` that allows configuration to override it's value.

    :param callable|str default: Default if config does not specify. If a callable is
    given it will be called.
    """

    hostname = _sentinel

    try:
        hostname = conf.get('core', 'hostname', fallback=_sentinel)
    except AirflowConfigException:
        pass

    if hostname is _sentinel:
        hostname = default() if callable(default) else default

    return hostname
