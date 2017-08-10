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

from airflow.logging_backends.base_logging_backend import BaseLoggingBackend
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search


class ElasticsearchLoggingBackend(BaseLoggingBackend):

    SCHEMA_NAME = 'elasticsearch'
    # Maximum number of logs to return per search call.
    MAX_PER_PAGE = 1000

    def __init__(self, host=None, **kwargs):
        super(ElasticsearchLoggingBackend, self).__init__(
            schema_name=self.SCHEMA_NAME, **kwargs)

        self.client = Elasticsearch([host])

        # Prevent adding ElasticSearch logs into task instance logs.
        # TODO: we should find a place to place ElasticSearch logs.
        logger = logging.getLogger('elasticsearch')
        logger.setLevel(logging.CRITICAL)

    def _search(self, **kwargs):
        """
        Return a list of documents given search conditions.
        :param dag_id: id of the dag
        :param task_id: id of the task
        :param execution_date: execution date of the task instance
        :param try_number: try_number of the task instance
        :param offset: filter log with offset strictly greater than offset
        :param page: logs at given page. Default value is 0.
        """

        dag_id = kwargs.get('dag_id')
        task_id = kwargs.get('task_id')
        execution_date = kwargs.get('execution_date')
        try_number = kwargs.get('try_number')
        offset = kwargs.get('offset')
        page = kwargs.get('page') or 0

        log_id = '-'.join([dag_id, task_id, execution_date, try_number])

        s = Search(using=self.client) \
            .query('match', log_id=log_id) \
            .sort('offset')

        # Offset is the unique key for sorting logs given log_id.
        if offset:
            s = s.filter('range', offset={'gt': offset})

        response = s[self.MAX_PER_PAGE * page:self.MAX_PER_PAGE].execute()

        return response

    def get_logs(self, **kwargs):
        response = self._search(**kwargs)
        logs = [hit for hit in response]
        return logs
