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


import logging

from pkg_resources import iter_entry_points
from airflow.models import Connection
from airflow.connection_types.base_connection_type import BaseConnectionType

for entry_point in iter_entry_points(group='airflow.connections', name=None):
    try:
        conn_type = entry_point.load()
        Connection.add_type(conn_type)
    except ImportError as e:
        logging.warning('Connection plugin error. Could not load %s', entry_point, exc_info=e)
