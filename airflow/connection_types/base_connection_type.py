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

import json
from urllib.parse import urlparse, urlunparse

class BaseConnectionType:
    """
    Information about a type of connection, it's name, it's description, and what hook it's linked to.
    Also used to parse/format data from and to an uri.
    The discovery of subclass is made using the entry_point : 'airflow.connections'
    """
    name = ''
    description = ''

    @classmethod
    def parse_from_uri(cls, uri):
        temp_uri = urlparse(uri)
        hostname = temp_uri.hostname or ''
        if '%2f' in hostname:
            hostname = hostname.replace('%2f', '/').replace('%2F', '/')
        # extras = dict([param.split('=', 1) for param in temp_uri.query.split('&')])
        return hostname, temp_uri.path[1:], temp_uri.username, temp_uri.password, temp_uri.port, None

    @classmethod
    def get_hook(cls, conn_id):
        pass

    @classmethod
    def to_uri(cls, hostname, schema, login, password, port, extra):
        host = '{}:{}'.format(hostname, port) if port else hostname
        user_infos = '{}:{}'.format(login, password) if login and password else None
        netloc = '{}@{}'.format(user_infos, host) if user_infos else host
        path = '/{}'.format(schema)
        params = json.loads(extra)
        query = '&'.join(['{}={}'.format(key, val) for key, val in params.items()])
        return urlunparse([cls.name, netloc, path, '', query, ''])
