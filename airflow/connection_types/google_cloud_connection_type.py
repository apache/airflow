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

from airflow.connection_types import BaseConnectionType

class GoogleCloudConnectionType(BaseConnectionType):


    name = 'google_cloud_platform'
    description = 'Google Cloud Platform'

    @classmethod
    def get_hook(cls, conn_id):
        try:
            from airflow.contrib.hooks.bigquery_hook import BigQueryHook
            return BigQueryHook(bigquery_conn_id=conn_id)
        except ImportError:
            pass
