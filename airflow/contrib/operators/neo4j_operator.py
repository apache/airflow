# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
from airflow.contrib.hooks.neo4j_hook import Neo4JHook
import csv


class Neo4JOperator(BaseOperator):
    """
    Neo4JOperator to interact and perform action on Neo4J graph database.
    This operator is designed to use Neo4J Python driver: https://neo4j.com/docs/api/python-driver/current/

    :param cypher_query: required cypher query to be executed on the Neo4J database
    :type cypher_query: str
    :param output_filename: required filename to produce with output from the query
    :type output_filename: str
    :param n4j_conn_id: reference to a pre-defined Neo4J Connection
    :type n4j_conn_id: str
    :param fail_on_no_results: True/False flag to indicate if it should fail the task if no results
    :type fail_on_no_results: bool
    """
    _cypher_query = None
    _output_filename = None
    _n4j_conn_id = None
    _fail_on_no_results = None

    @apply_defaults
    def __init__(self,
                 cypher_query,
                 output_filename,
                 n4j_conn_id='n4j_default',
                 fail_on_no_results=False,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)

        self._output_filename = output_filename
        self._cypher_query = cypher_query
        self._n4j_conn_id = n4j_conn_id
        self._fail_on_no_results = fail_on_no_results

    def execute(self, context):
        hook = Neo4JHook(n4j_conn_id=self._n4j_conn_id)
        if self._cypher_query is not None:
            result = hook.run_query(cypher_query=self._cypher_query)
        else:
            raise AirflowException("cypher_query is missing.")

        # In some cases, an empty result should fail (where results are expected)
        if result.peek() is None and self._fail_on_no_results:
            raise AirflowException("Query returned no rows")

        row_count = self._make_csv(result)

        # Provide some feedback to what was done...
        self.log.info("Saved {0} with {1} rows".format(self._output_filename, row_count))

        # result = 'neo4j.BoltStatementResult' See https://neo4j.com/docs/api/python-driver/current/results.html
        self.log.info("Processing output with keys: {}".format(result.keys()))

    def _make_csv(self, result):
        total_row_count = 0

        # Consider available disk space on the Airflow server, maybe support S3 bucket and bring in the S3 Hook
        with open(self._output_filename, 'w', newline='') as output_file:
            output_writer = csv.DictWriter(output_file, fieldnames=result.keys())
            output_writer.writeheader()

            for total_row_count, row in enumerate(result, start=1):
                # row = 'neo4j.Record'
                output_writer.writerow(row.data())

        return total_row_count
