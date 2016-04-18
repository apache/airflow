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
from airflow.common.client.client import Client
from airflow.common.variables import get_var, set_var, list_var


class LocalClient(Client):
    def __init__(self):
        pass

    def get_var(self, key, default_var=None, deserialize_json=False):
        return get_var(key, default_var, deserialize_json)

    def set_var(self, key, value, serialize_json=True):
        set_var(key, value, serialize_json)

    def list_var(self):
        return list_var()

