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
from typing import List, Optional

from flask import Response, request
from marshmallow import ValidationError
from sqlalchemy import and_, func
from sqlalchemy.orm.session import Session

from airflow.api_connexion import parameters
from airflow.api_connexion.exceptions import BadRequest, NotFound
from airflow.api_connexion.schemas.xcom_schema import (
    XComCollection, XComCollectionItemSchema, XComCollectionSchema, xcom_collection_item_schema,
    xcom_collection_schema, xcom_schema,
)
from airflow.models import DagRun as DR, XCom
from airflow.utils.session import provide_session


@provide_session
def delete_xcom_entry(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    xcom_key: str,
    session: Session
):
    """
    Delete an XCom entry
    """

    dag_run = session.query(DR).filter(DR.run_id == dag_run_id,
                                       DR.dag_id == dag_id).first()
    if not dag_run:
        raise NotFound(f'DAGRun with dag_id:{dag_id} and run_id:{dag_run_id} not found')

    entry = session.query(XCom).filter(XCom.dag_id == dag_id,
                                       XCom.task_id == task_id,
                                       XCom.key == xcom_key).delete()
    if not entry:
        raise NotFound("XCom entry not found")

    return Response('', 204)


@provide_session
def get_xcom_entries(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    session: Session
) -> XComCollectionSchema:
    """
    Get all XCom values
    """
    offset = request.args.get(parameters.page_offset, 0)
    limit = min(int(request.args.get(parameters.page_limit, 100)), 100)
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


@provide_session
def patch_xcom_entry(
    session: Session,
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    xcom_key: str,
    update_mask: Optional[List[str]] = None
):
    """
    Update an XCom entry
    """
    dag_run = session.query(DR).filter(DR.run_id == dag_run_id,
                                       DR.dag_id == dag_id).first()
    if not dag_run:
        raise NotFound(f'DAGRun with dag_id:{dag_id} and run_id:{dag_run_id} not found')

    xcom_entry = session.query(XCom).filter(XCom.dag_id == dag_id,
                                            XCom.task_id == task_id,
                                            XCom.key == xcom_key).first()

    if not xcom_entry:
        raise NotFound("XCom not found")
    try:
        body = xcom_schema.load(request.json, partial=("dag_id", "task_id",
                                                       "key", "execution_date"))
    except ValidationError as err:
        raise BadRequest(detail=err.messages.get('_schema', [str(err.messages)])[0])
    data = body.data
    # Check that other attributes are not being updated
    for field in data:
        if field != 'value' and data[field] != getattr(xcom_entry, field):
            raise BadRequest(detail=f"'{field}' cannot be updated")
    if update_mask:
        update_mask = [i.strip() for i in update_mask]
        data_ = {}
        for field in update_mask:
            if field in data and field != 'value':
                raise BadRequest(detail=f"{field} cannot be updated")
            elif field in data and field == 'value':
                data_[field] = data[field]
            else:
                raise BadRequest(detail=f"Unknown field '{field}' in update_mask")
        data = data_
    data['value'] = XCom.serialize_value(data['value'])
    xcom_entry.value = data['value']
    session.add(xcom_entry)
    session.commit()
    return xcom_schema.dump(xcom_entry)


@provide_session
def post_xcom_entries(dag_id: str,
                      dag_run_id: str,
                      task_id: str,
                      session: Session
                      ):
    """
    Create an XCom entry
    """

    dag_run = session.query(DR).filter(DR.run_id == dag_run_id, DR.dag_id == dag_id).first()
    if not dag_run:
        raise NotFound(f'DAGRun with dag_id:{dag_id} and run_id:{dag_run_id} not found')
    try:
        xcom_entry = xcom_schema.load(request.json)
    except ValidationError as err:
        raise BadRequest(detail=err.messages.get('_schema', [str(err.messages)])[0])
    session.query(XCom).filter(XCom.dag_id == dag_id, XCom.task_id == task_id).delete()
    data = xcom_entry.data
    data['value'] = XCom.serialize_value(data['value'])
    xcom = XCom(**data)
    session.add(xcom)
    session.commit()
    return xcom_schema.dump(xcom)
