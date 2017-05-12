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

from airflow.hooks.base_hook import BaseHook
from airflow import configuration

from elasticsearch import Elasticsearch, ElasticsearchException
from airflow.exceptions import AirflowException

class ElasticsearchHook(BaseHook, Elasticsearch):
    """
    interact with elasticsearch.

    an instance of such class will come with all the elasticsearch
    python client goodies via multiple inheritance. 
    """
    def __init__(
            self,
            es_conn_id = 'elasticsearch_default',
            **kwargs):
        self.es_conn_id = es_conn_id
        host_domain = self.get_connections(self.es_conn_id)
        super(Elasticsearch, self).__init__(
            host_domain,
            **kwargs) 
            
