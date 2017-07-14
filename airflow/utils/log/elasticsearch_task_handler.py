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

from airflow import AirflowException
from airflow.utils.log.file_task_handler import FileTaskHandler
from elasticsearch import Elasticsearch, ElasticsearchException, helpers
from elasticsearch_dsl import Search


class ElasticsearchTaskHandler(FileTaskHandler):
    """
    ElasticsearchTaskHandler is a python log handler that
    reads logs from Elasticsearch. Note logs are not directly
    indexed into Elasticsearch. Instead, it flushes logs
    into local files. Additional software setup is required
    to index the log into Elasticsearch, such as using
    Filebeat and Logstash.

    To efficiently query and sort Elasticsearch results, we assume each
    log message has a field `log_id` consists of ti primary keys:
    `log_id = {dag_id}-{task_id}-{execution_date}-{try_number}`
    Log messages with specific log_id are sorted based on `offset`,
    which is a unique integer indicates log message's order.
    Timestamp here are unreliable because multiple log messages
    might have the same timestamp.
    """
    def __init__(self, base_log_folder, filename_template,
                 host='localhost:9200'):
        """
        :param base_log_folder: base folder to store logs locally
        :param filename_template: log filename template
        :param host: Elasticsearh host name
        """
        super(ElasticsearchTaskHandler, self).__init__(
            base_log_folder, filename_template)
        self.client = Elasticsearch([host])

    def streaming_read(self, dag_id, task_id, execution_date,
                  try_number, offset=None, page=0, max_line_per_page=1000):
        """
        Endpoint for streaming log.
        :param dag_id: id of the dag
        :param task_id: id of the task
        :param execution_date: execution date in isoformat
        :param try_number: try_number of the task instance
        :param offset: filter log with offset strictly greater than offset
        :param page: logs at given page
        :param max_line_per_page: maximum number of results returned per ES query
        :return a list of log documents
        """
        log_id = '-'.join([dag_id, task_id, execution_date, try_number])

        s = Search(using=self.client) \
            .query('match', log_id=log_id) \
            .sort('offset')

        # Offset is the unique key for sorting logs given log_id.
        if offset:
            s = s.filter('range', offset={'gt': offset})

        try:
            response = s[max_line_per_page * page:max_line_per_page].execute()
            logs = [hit for hit in response]

        except ElasticsearchException as e:
            # Do not swallow the ES error.
            err = "Unable to read logs from ElasticSearch: {}\n".format(str(e))
            raise AirflowException(err)

        return logs

    def _read(self, ti, try_number):
        """
        Read all logs of given task instance and try_number from Elaticsearch.
        :param ti: task instance object
        :param try_number: task instance try_number to read logs from
        :return return log messages in string format
        """
        dag_id = ti.dag_id
        task_id = ti.task_id
        execution_date = ti.execution_date.isoformat()

        log_id = '-'.join([dag_id, task_id, execution_date, try_number])

        # Use ES Scroll API to get all logs, since query DSL can at most
        # return 10k results. This might take some time as sorting scroll
        # results is very expensive. Please use streaming endpoint to boost
        # performance.
        s = Search(using=self.client) \
            .query('match', log_id=log_id) \
            .sort('offset')
        query = s.to_dict()
        try:
            response = helpers.scan(self.client, query=query, preserve_order=True)
            log = '\n'.join([hit['_source']['message'] for hit in response])
        except ElasticsearchException as e:
            err = "Unable to read logs from ElasticSearch: {}\n".format(str(e))
            raise AirflowException(err)

        return log
