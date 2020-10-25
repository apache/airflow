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
"""Lineage apis"""
import datetime
from typing import Any, Dict, List

from airflow.api.common.experimental import check_and_get_dag, check_and_get_dagrun
from airflow.lineage import PIPELINE_INLETS, PIPELINE_OUTLETS
from airflow.models.xcom import XCom
from airflow.utils.session import provide_session


@provide_session
def get_lineage(dag_id: str, execution_date: datetime.datetime, session=None) -> Dict[str, Dict[str, Any]]:
    """Gets the lineage information for dag specified"""
    dag = check_and_get_dag(dag_id)
    check_and_get_dagrun(dag, execution_date)

    inlets: List[XCom] = XCom.get_many(dag_ids=dag_id, execution_date=execution_date,
                                       key=PIPELINE_INLETS, session=session).all()
    outlets: List[XCom] = XCom.get_many(dag_ids=dag_id, execution_date=execution_date,
                                        key=PIPELINE_OUTLETS, session=session).all()

    lineage: Dict[str, Dict[str, Any]] = {}
    for meta in inlets:
        lineage[meta.task_id] = {'inlets': meta.value}

    for meta in outlets:
        lineage[meta.task_id]['outlets'] = meta.value

    return {'task_ids': lineage}
