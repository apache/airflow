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


class Client:
    def __init__(self, api_base_url, auth):
        self._api_base_url = api_base_url
        self._auth = auth

    def trigger_dag(self, dag_id, run_id=None, conf=None, execution_date=None):
        """
        Creates a dag run for the specified dag
        :param dag_id:
        :param run_id:
        :param conf:
        :return:
        """
        raise NotImplementedError()
