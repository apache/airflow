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
from future.moves.urllib.parse import urljoin

from airflow.api.client import api_client

import requests


class Client(api_client.Client):
    def trigger_dag(self, dag_id, run_id=None, conf=None, execution_date=None):
        endpoint = '/api/experimental/dags/{}/dag_runs'.format(dag_id)
        url = urljoin(self._api_base_url, endpoint)

        resp = requests.post(url,
                             auth=self._auth,
                             json={
                                 "run_id": run_id,
                                 "conf": conf,
                                 "execution_date": execution_date,
                             })

        if not resp.ok:
            raise IOError()

        data = resp.json()

        return data['message']
