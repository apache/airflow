# -*- coding: utf-8 -*-
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
#
import importlib
import socket

from airflow.configuration import AirflowConfigException, conf


def get_host_ip_address():
    """
    Fetch host ip address.
    """
    return socket.gethostbyname(socket.getfqdn())


def get_hostname():
    """
    Fetch the hostname using the callable from the config or using
    `socket.getfqdn` as a fallback.
    """
    # First we attempt to fetch the callable path from the config.
    try:
        callable_path = conf.get('core', 'hostname_callable')
    except AirflowConfigException:
        callable_path = None

    # Then we handle the case when the config is missing or empty. This is the
    # default behavior.
    if not callable_path:
        return socket.getfqdn()

    # Since we have a callable path, we try to import and run it next.
    module_path, attr_name = callable_path.split(':')
    module = importlib.import_module(module_path)
    func = getattr(module, attr_name)
    return func()
