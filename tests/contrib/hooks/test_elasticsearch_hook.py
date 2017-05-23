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
import unittest
from airflow.contrib.hooks.elasticsearch_hook import ElasticsearchHook


class TestElasticsearchHook(unittest.TestCase):
    def test_get_conn_with_endpoint(self):
        hook = ElasticsearchHook()
        es_client = hook.get_conn(endpoint_url='localhost:9200')

        # check if the hook has the elasticsearch client functions:
        self.assertTrue(hashattr(es_client, 'msearch'))

    def test_get_conn_with_configuration(self):
        hook = Elasticsearch()
        es_client = hook.get_conn()

        self.assertTrue(hashattr(es_client, 'msearch'))


if __name__ == '__main__':
    unittest.main()
