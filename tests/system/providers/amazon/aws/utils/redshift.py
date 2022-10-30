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

from __future__ import annotations

from airflow import settings
from airflow.decorators import task
from airflow.models import Connection
from airflow.providers.amazon.aws.hooks.redshift_cluster import RedshiftHook


@task
def create_connection(conn_id_name: str, cluster_id: str, db_login: str, db_pass: str):
    redshift_hook = RedshiftHook()
    cluster_endpoint = redshift_hook.get_conn().describe_clusters(ClusterIdentifier=cluster_id)['Clusters'][0]
    conn = Connection(
        conn_id=conn_id_name,
        conn_type='redshift',
        host=cluster_endpoint['Endpoint']['Address'],
        login=db_login,
        password=db_pass,
        port=cluster_endpoint['Endpoint']['Port'],
        schema=cluster_endpoint['DBName'],
    )
    session = settings.Session()
    session.add(conn)
    session.commit()
