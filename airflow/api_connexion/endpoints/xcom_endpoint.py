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

from sqlalchemy import and_

from airflow.api_connexion.schemas.xcom_schema import xcom_collection_item_schema
from airflow.models import DagRun as DR, XCom
from airflow.utils.session import provide_session


def delete_xcom_entry():
    """
    Delete an XCom entry
    """
    raise NotImplementedError("Not implemented yet.")


def get_xcom_entries():
    """
    Get all XCom values
    """
    raise NotImplementedError("Not implemented yet.")


@provide_session
def get_xcom_entry(dag_id, task_id, dag_run_id, xcom_key, session):
    """
    Get an XCom entry
    """
    query = session.query(XCom)
    query = query.filter(and_(XCom.dag_id == dag_id,
                              XCom.task_id == task_id,
                              XCom.key == xcom_key))
    query = query.join(DR, and_(XCom.dag_id == DR.dag_id, XCom.execution_date == DR.execution_date))
    query = query.filter(DR.run_id == dag_run_id)

    q_object = query.one_or_none()
    if not q_object:
        raise Exception("Object Not found")
    return xcom_collection_item_schema.dump(q_object)


def patch_xcom_entry():
    """
    Update an XCom entry
    """
    raise NotImplementedError("Not implemented yet.")


def post_xcom_entries():
    """
    Create an XCom entry
    """
    raise NotImplementedError("Not implemented yet.")
