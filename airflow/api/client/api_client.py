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

import airflow.api.common.proto.variable_pb2 as variable_pb2
import airflow.models as models

from urlparse import urljoin

from airflow.common.client.client import Client


class ApiClient(Client):
    def __init__(self, api_base_url):
        self.api_base_url = api_base_url
        pass

    def get_var(self, key, default_var=None, deserialize_json=False):
        url = urljoin(self.api_base_url, "/api/v1/variables/get/{}".format(key))
        resp = requests.get(url)

        if not resp.ok:
            raise ValueError()

        var = variable_pb2.Variable()
        var.ParseFromString(resp.content)

        m_var = models.Variable()
        m_var.key = var.key
        m_var.set_val(var.value)

        return m_var

    def set_var(self, key, value, serialize_json=False):
        url = urljoin(self.api_base_url, "/api/v1/variables/set")
        var = variable_pb2.Variable()
        var.key = key
        var.value = value

        resp = requests.post(url, data=var.SerializeToString())

        assert resp.ok

    def list_var(self):
        url = urljoin(self.api_base_url, "/api/v1/variables/list")
        resp = requests.get(url)

        if not resp.ok:
            raise IOError()

        var_list = variable_pb2.VariableList()
        var_list.ParseFromString(resp.content)

        v = []
        for pb_var in var_list.variables:
            m_var = models.Variable()
            m_var.key = pb_var.key
            m_var.set_val(pb_var.value)
            v.append(m_var)

        return v

