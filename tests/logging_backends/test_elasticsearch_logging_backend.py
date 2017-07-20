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

import json
import os
import time
import unittest

if 'AIRFLOW_RUNALL_TESTS' in os.environ:

    from airflow.logging_backends.elasticsearch_logging_backend import ElasticsearchLoggingBackend
    from elasticsearch import Elasticsearch


    class TestElasticsearchLoggingBackend(unittest.TestCase):

        INDEX_NAME = "airflow"
        TEMPLATE_NAME = "airflow"
        TEMPLATE_FILENAME = os.path.join(os.path.dirname(__file__), 'test_elasticsearch_template.json')
        DOC_TYPE = "log"

        def setUp(self):
            self.host = "localhost:9200"
            self.backend = ElasticsearchLoggingBackend(host=self.host)
            self.es = self.backend.client
            self.doc1 = {
                "log_timestamp": "2017-06-19 15:08:17,077",
                "log_filename": "cli.py",
                "log_details": "Running on host",
                "offset": "76",
                "execution_date": "2017-06-13T00:00:00",
                "input_type": "log",
                "log_level": "INFO",
                "task_id": "one_success",
                "source": "airflow/logs/example_skip_dag/one_success/2017-06-13T00:00:00",
                "message": "[2017-06-19 15:08:17,077] {cli.py:389} INFO - Running on host",
                "type": "log",
                "file_id": "example_skip_dag-one_success-2017-06-13T00:00:00",
                "log_line_number": "389",
                "dag_id": "example_skip_dag"
            }

            self.doc2 = {
                "log_timestamp": "2017-06-19 15:08:17,077",
                "log_filename": "models.py",
                "log_details": "Filling up the DagBag from airflow/airflow/example_dags/example_skip_dag.py",
                "offset": "221",
                "execution_date": "2017-06-13T00:00:00",
                "input_type": "log",
                "log_level": "INFO",
                "task_id": "one_success",
                "source": "airflow/logs/example_skip_dag/one_success/2017-06-13T00:00:00",
                "message": "[2017-06-19 15:08:17,077] {models.py:176} INFO - Filling up the DagBag from airflow/airflow/example_dags/example_skip_dag.py",
                "type": "log",
                "file_id": "example_skip_dag-one_success-2017-06-13T00:00:00",
                "log_line_number": "176",
                "dag_id": "example_skip_dag"
            }

            self.doc3 = {
                "log_timestamp": "2017-06-19 15:08:18,077",
                "log_filename": "base_task_runner.py",
                "log_details": "Running: ['bash', '-c', u'airflow run example_skip_dag one_success 2017-06-13T00:00:00 --job_id 101 --raw -sd DAGS_FOLDER/example_dags/example_skip_dag.py']",
                "offset": "437",
                "execution_date": "2017-06-13T00:00:00",
                "input_type": "log",
                "log_level": "INFO",
                "task_id": "one_success",
                "source": "airflow/logs/example_skip_dag/one_success/2017-06-13T00:00:00",
                "message": "[2017-06-19 15:08:18,077] {base_task_runner.py:112} INFO - Running: ['bash', '-c', u'airflow run example_skip_dag one_success 2017-06-13T00:00:00 --job_id 101 --raw -sd DAGS_FOLDER/example_dags/example_skip_dag.py']",
                "type": "log",
                "file_id": "example_skip_dag-one_success-2017-06-13T00:00:00",
                "log_line_number": "112",
                "dag_id": "example_skip_dag"
            }

            # Load Elasticsearch 5 test template
            self.template = {
                "template" : "*",
                "mappings": {
                    "_default_": {
                        "_all": { "enabled": "false" },
                        "dynamic_date_formats": [
                            "strict_date_optional_time",
                            "yyyy/MM/dd HH:mm:ss Z||yyyy/MM/dd Z",
                            "yyyy-MM-dd HH:mm:ss,SSS"
                        ],
                        "numeric_detection": "true",
                        "dynamic_templates": [
                            {
                                "timestamp_template" : {
                                    "path_match": "*",
                                    "match_mapping_type": "date",
                                    "mapping": {
                                        "type": "date",
                                        "index": "not_analyzed",
                                        "doc_values": "true",
                                        "ignore_malformed": "true"
                                    }
                                }
                            },
                            {
                                "integer_template": {
                                    "path_match": "*",
                                    "match_mapping_type": "long",
                                    "mapping": {
                                        "type": "integer",
                                        "index": "not_analyzed",
                                        "doc_values": "true"
                                    }
                                }
                            },
                            {
                                "default_template" : {
                                    "path_match": "*",
                                    "match_mapping_type": "string",
                                    "mapping": {
                                        "type": "keyword",
                                        "index": "not_analyzed",
                                        "doc_values": "true"
                                    }
                                }
                            }
                         ],
                         "properties" : {
                            "message" : { "type" : "text" }
                        }
                    }
                }
            }

            # Make sure Elasticsearch has no old index, template and pipeline.
            self.es.indices.delete(index="*", ignore=[404, 400])
            self.es.indices.delete_template(name="*")
            self.es.ingest.delete_pipeline(id="*")
            # Add test template
            self.es.indices.put_template(name=self.TEMPLATE_NAME, body=self.template)
            # Index documents
            self.es.index(index=self.INDEX_NAME, doc_type=self.DOC_TYPE, id=1, body=self.doc1)
            self.es.index(index=self.INDEX_NAME, doc_type=self.DOC_TYPE, id=2, body=self.doc2)
            self.es.index(index=self.INDEX_NAME, doc_type=self.DOC_TYPE, id=3, body=self.doc3)
            # Give Elasticsearch sometime to index document
            time.sleep(3)

        def test_get_logs(self):
            dag_id = "example_skip_dag"
            task_id = "one_success"
            execution_date = "2017-06-13T00:00:00"

            logs = self.backend.get_logs(dag_id, task_id, execution_date)

            self.assertEqual(len(logs), 3)
            # Check logs are sorted
            self.assertEqual(logs[0].message, self.doc1['message'])
            self.assertEqual(logs[1].message, self.doc2['message'])
            self.assertEqual(logs[2].message, self.doc3['message'])

    if __name__ == "__main__":
        unittest.main()
