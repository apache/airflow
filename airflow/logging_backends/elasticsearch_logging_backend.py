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

from airflow.logging_backends.base_logging_backend import BaseLoggingBackend, Log
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search


class ElasticsearchLoggingBackend(BaseLoggingBackend):
    """
    This search backend requires ES document to have the following schema:
    {
        "log_timestamp": "2017-06-19 15:08:17,077", // primary soring key
        "log_filename": "cli.py",
        "log_line_number": "389",
        "log_level": "INFO",
        "log_details": "Running on host",
        "offset": "76",  // secondary sorting key when log_timestamp is the same
        "dag_id": "example_skip_dag"
        "task_id": "one_success",
        "execution_date": "2017-06-13T00:00:00",
        "message": "[2017-06-19 15:08:17,077] {cli.py:389} INFO - Running on host",
        "file_id": "example_skip_dag-one_success-2017-06-13T00:00:00",
    }
    """

    SCHEMA_NAME = 'elasticsearch'

    def __init__(self, host=None, **kwargs):
        super(ElasticsearchLoggingBackend, self).__init__(
            schema_name=self.SCHEMA_NAME, **kwargs)

        self.client = Elasticsearch([host])

        # Disable elasticsearch logger unless CRITICAL level
        logger = logging.getLogger('elasticsearch')
        logger.setLevel(logging.CRITICAL)

    def _search(self, **kwargs):
        """
        Return a list of documents given search conditions.
        :arg dag_id: id of the dag
        :arg task_id: id of the task
        :arg execution_date: execution date of the dag run
        :arg gt_ts: log has log_timestamp greater than gt_ts
        :arg gte_ts: log has log_timestamp greater than or equal to gte_ts
        :arg lt_ts: log has log_timestamp less than lt_ts
        :arg lte_ts: log has log_timestamp less than or equal to lte_ts
        :arg page: logs at given page
        :arg max_page_size: maximum log lines returned in one page
        """

        dag_id = kwargs.get('dag_id')
        task_id = kwargs.get('task_id')
        execution_date = kwargs.get('execution_date')
        gt_ts = kwargs.get('gt_ts')
        gte_ts = kwargs.get('gte_ts')
        lt_ts = kwargs.get('lt_ts')
        lte_ts = kwargs.get('lte_ts')
        page = kwargs.get('page') or 0
        max_page_size = kwargs.get('max_page_size') or 10000

        file_id = '-'.join([dag_id, task_id, execution_date])

        s = Search(using=self.client) \
            .query('match', file_id=file_id) \
            .sort('log_timestamp', 'offset')

        if lt_ts:
            s = s.filter('range', log_timestamp={'lt': lt_ts})

        if lte_ts:
            s = s.filter('range', log_timestamp={'lte': lte_ts})

        if gt_ts:
            s = s.filter('range', log_timestamp={'gt': gt_ts})

        if gte_ts:
            s = s.filter('range', log_timestamp={'gte': gte_ts})

        response = s[max_page_size * page:max_page_size].execute()

        return response

    def get_logs(self, dag_id, task_id, execution_date, **kwargs):
        response = self._search(dag_id=dag_id, task_id=task_id,
                                execution_date=execution_date, **kwargs)
        logs = [ESLog(hit) for hit in response]
        return logs

class ESLog(Log):
    def __init__(self, log):
        super(ESLog, self).__init__(log)

    @property
    def message(self):
        return self.log.message

    @property
    def timestamp(self):
        return self.log.log_timestamp

    @property
    def filename(self):
        return self.log.log_filename

    @property
    def fileline(self):
        return self.log.log_line_number

    @property
    def log_level(self):
        return self.log.log_level

    @property
    def details(self):
        return self.log.log_details
