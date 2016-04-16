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
import requests
import json

from urlparse import urljoin

from airflow.common.client.client import Client
from airflow.models import Variable


class ApiClient(Client):
    def __init__(self, api_base_url):
        self.api_base_url = api_base_url
        pass

    def get_var(self, key, default_var=None, deserialize_json=False):
        url = urljoin(self.api_base_url, "/api/v1/variables/get/{}/{}/{}".format(key, default_var, deserialize_json))
        resp = requests.get(url)

        if not resp.ok:
            raise ValueError()

        j_variable = resp.json('variable')

        return Variable(j_variable['key'], j_variable['value'])

    def set_var(self, key, value, serialize_json=False):
        url = urljoin(self.api_base_url, "/api/v1/variables/set")
        resp = requests.post(url, data = json.dumps({'key': key,
                                                     'value': value,
                                                     'serialize_json': serialize_json}))

        assert resp.ok

    def list_var(self):
        url = urljoin(self.api_base_url, "/api/v1/variables/list")
        resp = requests.get(url)

        if not resp.ok:
            raise IOError()

        # todo: return correct deserialized data
        return resp.json('variables')

