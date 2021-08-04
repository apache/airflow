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


from airflow.models.dag import DagModel

from airflow.www.views import (
    build_dag_sorting_query,
    dag_query_for_key,
    query_ordering_transform_for_key,
)


def test_dag_sorting_query():

    dag_id_query = dag_query_for_key(sorting_key='dag_id')
    dag_id_query_casetest = dag_query_for_key(sorting_key='dAg_Id')

    assert dag_id_query == DagModel.dag_id
    assert dag_id_query_casetest == dag_id_query

    owner_query = dag_query_for_key(sorting_key='owner')
    owner_query_casetest = dag_query_for_key(sorting_key='oWnEr')

    assert owner_query == DagModel.owners
    assert owner_query != dag_id_query
    assert owner_query_casetest == owner_query

    schedule_query = dag_query_for_key(sorting_key='next_dagrun')

    assert schedule_query == DagModel.next_dagrun
    assert schedule_query != dag_id_query
    assert schedule_query != owner_query

    asc_transform = query_ordering_transform_for_key(sorting_order='asc')
    asc_transform_casetest = query_ordering_transform_for_key(sorting_order='aSC')

    assert asc_transform is not None
    assert asc_transform == asc_transform_casetest

    desc_transform = query_ordering_transform_for_key(sorting_order='desc')
    desc_transform_casetest = query_ordering_transform_for_key(sorting_order='DeSC')

    assert desc_transform is not None
    assert desc_transform == desc_transform_casetest

    assert asc_transform != desc_transform
    assert asc_transform_casetest != desc_transform_casetest

    asc_dag_id_query = build_dag_sorting_query(sorting_key='dag_id', sorting_order='asc')
    assert asc_dag_id_query is not None

    desc_dag_id_query = build_dag_sorting_query(sorting_key='dag_id', sorting_order='desc')
    assert desc_dag_id_query is not None

    asc_schedule_query = build_dag_sorting_query(sorting_key='nEXt_dAgEun', sorting_order='aSc')
    asc_owner_query = build_dag_sorting_query(sorting_key='oWNer', sorting_order='aSc')
    desc_owner_query = build_dag_sorting_query(sorting_key='oWNer', sorting_order='deSC')

    assert asc_schedule_query is not None
    assert asc_owner_query is not None
    assert desc_owner_query is not None

    assert asc_schedule_query != asc_owner_query
    assert desc_owner_query != asc_owner_query
    assert asc_schedule_query != asc_dag_id_query
    assert asc_owner_query != asc_dag_id_query
