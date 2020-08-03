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
from typing import Optional

from sqlalchemy import and_, func
from sqlalchemy.orm.session import Session

from airflow.api_connexion.exceptions import NotFound
from airflow.api_connexion.parameters import check_limit, format_parameters
from airflow.api_connexion.schemas.xcom_schema import (
    XComCollection, XComCollectionItemSchema, XComCollectionSchema, xcom_collection_item_schema,
    xcom_collection_schema,
)
from airflow.models import DagRun as DR, XCom
from airflow.utils.session import provide_session


@format_parameters({
    'limit': check_limit
})
@provide_session
def get_xcom_entries(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    session: Session,
    limit: Optional[int],
    offset: Optional[int] = None
) -> XComCollectionSchema:
    """
    Get all XCom values
    """

    query = session.query(XCom)
    if dag_id != '~':
        query = query.filter(XCom.dag_id == dag_id)
        query.join(DR, and_(XCom.dag_id == DR.dag_id, XCom.execution_date == DR.execution_date))
    else:
        query.join(DR, XCom.execution_date == DR.execution_date)
    if task_id != '~':
        query = query.filter(XCom.task_id == task_id)
    if dag_run_id != '~':
        query = query.filter(DR.run_id == dag_run_id)
    query = query.order_by(
        XCom.execution_date, XCom.task_id, XCom.dag_id, XCom.key
    )
    total_entries = session.query(func.count(XCom.key)).scalar()
    query = query.offset(offset).limit(limit)
    return xcom_collection_schema.dump(XComCollection(xcom_entries=query.all(), total_entries=total_entries))


@provide_session
def get_xcom_entry(
    dag_id: str,
    task_id: str,
    dag_run_id: str,
    xcom_key: str,
    session: Session
) -> XComCollectionItemSchema:
    """
    Get an XCom entry
    """
    query = session.query(XCom)
    query = query.filter(and_(XCom.dag_id == dag_id,
                              XCom.task_id == task_id,
                              XCom.key == xcom_key))
    query = query.join(DR, and_(XCom.dag_id == DR.dag_id, XCom.execution_date == DR.execution_date))
    query = query.filter(DR.run_id == dag_run_id)

    query_object = query.one_or_none()
    if not query_object:
        raise NotFound("XCom entry not found")
    return xcom_collection_item_schema.dump(query_object)
