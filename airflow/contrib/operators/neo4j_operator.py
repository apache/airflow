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
"""
Neo4JOperator to interact and perform action on Neo4J graph database.
This operator is designed to use Neo4J Hook and the
Python driver: https://neo4j.com/docs/api/python-driver/current/
"""

import csv
from os.path import isfile

from neo4j import BoltStatementResult

from airflow.contrib.hooks.neo4j_hook import Neo4JHook
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class Neo4JOperator(BaseOperator):
    """
    This operator provides Airflow DAGs the ability to execute a cypher query
    and save the results of the query to a CSV file.
    :param cypher_query: required cypher query to be executed on the Neo4J database
    :type cypher_query: str
    :param output_filename: required filename to produce with output from the query
    :type output_filename: str
    :param n4j_conn_id: reference to a pre-defined Neo4J Connection
    :type n4j_conn_id: str
    :param soft_fail: set True to fail when query return no result
    :type soft_fail: bool
    """
    cypher_query = None
    output_filename = None
    n4j_conn_id = None
    soft_fail = None

    template_fields = ['cypher_query', 'output_filename', 'n4j_conn_id', 'soft_fail']

    @apply_defaults
    def __init__(self,
                 cypher_query,
                 output_filename,
                 n4j_conn_id,
                 soft_fail=False,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)

        self.output_filename = output_filename
        self.cypher_query = cypher_query
        self.n4j_conn_id = n4j_conn_id
        self.soft_fail = soft_fail

    def execute(self, context):
        """
        Executes the supplied query and saves the results as a CSV file on disk
        :param context:
        :return:
        """
        if isfile(self.cypher_query):
            with open(self.cypher_query, 'r') as input_file:
                self.cypher_query = input_file.read()

        hook = Neo4JHook(n4j_conn_id=self.n4j_conn_id)
        result: BoltStatementResult = hook.run_query(cypher_query=self.cypher_query)

        # In some cases, an empty result should fail (where results are expected)
        if result.peek() is None and self.soft_fail:
            raise AirflowException("Query returned no rows")

        row_count = self._make_csv(result)

        self.log.info("Saved %s with %s rows", self.output_filename, row_count)

        return row_count

    def _make_csv(self, result: BoltStatementResult):
        """
        Local utility method to write out the results of query execution
        to a CSV. Better options could be added in the future
        :param result: Result of query execution
        :return: int: Count of rows written
        """
        total_row_count = 0

        if self.output_filename is not None:
            with open(self.output_filename, 'w', newline='') as output_file:
                output_writer = csv.DictWriter(output_file, fieldnames=result.keys())
                output_writer.writeheader()

                for total_row_count, row in enumerate(result, start=1):
                    # row = 'neo4j.Record'
                    output_writer.writerow(row.data())
        else:
            raise AirflowException("Must supply an output_filename")

        return total_row_count
