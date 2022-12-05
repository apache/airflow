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
"""Local client API."""
from __future__ import annotations

from airflow.api.client import api_client
from airflow.api.common import delete_dag, trigger_dag
from airflow.api.common.experimental.get_lineage import get_lineage as get_lineage_api
from airflow.exceptions import AirflowBadRequest, PoolNotFound
from airflow.models.pool import Pool


class Client(api_client.Client):
    """Local API client implementation."""

    def trigger_dag(self, dag_id, run_id=None, conf=None, execution_date=None, replace_microseconds=True):
        dag_run = trigger_dag.trigger_dag(
            dag_id=dag_id,
            run_id=run_id,
            conf=conf,
            execution_date=execution_date,
            replace_microseconds=replace_microseconds,
        )
        return f"Created {dag_run}"

    def delete_dag(self, dag_id):
        count = delete_dag.delete_dag(dag_id)
        return f"Removed {count} record(s)"

    def get_pool(self, name):
        pool = Pool.get_pool(pool_name=name)
        if not pool:
            raise PoolNotFound(f"Pool {name} not found")
        return pool.pool, pool.slots, pool.description

    def get_pools(self):
        return [(p.pool, p.slots, p.description) for p in Pool.get_pools()]

    def create_pool(self, name, slots, description):
        if not (name and name.strip()):
            raise AirflowBadRequest("Pool name shouldn't be empty")
        pool_name_length = Pool.pool.property.columns[0].type.length
        if len(name) > pool_name_length:
            raise AirflowBadRequest(f"pool name cannot be more than {pool_name_length} characters")
        try:
            slots = int(slots)
        except ValueError:
            raise AirflowBadRequest(f"Bad value for `slots`: {slots}")
        pool = Pool.create_or_update_pool(name=name, slots=slots, description=description)
        return pool.pool, pool.slots, pool.description

    def delete_pool(self, name):
        pool = Pool.delete_pool(name=name)
        return pool.pool, pool.slots, pool.description

    def get_lineage(self, dag_id, execution_date):
        lineage = get_lineage_api(dag_id=dag_id, execution_date=execution_date)
        return lineage
