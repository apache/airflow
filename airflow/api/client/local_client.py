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
"""Local client API"""

from airflow.api.client import api_client
from airflow.api.common.experimental import delete_dag, pool, trigger_dag
from airflow.api.common.experimental.get_lineage import get_lineage as get_lineage_api


class Client(api_client.Client):
    """Local API client implementation."""

    def trigger_dag(self, dag_id, run_id=None, conf=None, execution_date=None):
        dag_run = trigger_dag.trigger_dag(dag_id=dag_id,
                                          run_id=run_id,
                                          conf=conf,
                                          execution_date=execution_date)
        return "Created {}".format(dag_run)

    def delete_dag(self, dag_id):
        count = delete_dag.delete_dag(dag_id)
        return "Removed {} record(s)".format(count)

    def get_pool(self, name):
        the_pool = pool.get_pool(name=name)
        return the_pool.pool, the_pool.slots, the_pool.description

    def get_pools(self):
        return [(p.pool, p.slots, p.description) for p in pool.get_pools()]

    def create_pool(self, name, slots, description):
        the_pool = pool.create_pool(name=name, slots=slots, description=description)
        return the_pool.pool, the_pool.slots, the_pool.description

    def delete_pool(self, name):
        the_pool = pool.delete_pool(name=name)
        return the_pool.pool, the_pool.slots, the_pool.description

    def get_lineage(self, dag_id, execution_date):
        lineage = get_lineage_api(dag_id=dag_id, execution_date=execution_date)
        return lineage
