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


from airflow.security.permissions import RESOURCE_DAG_PREFIX
from airflow.utils.session import create_session
from airflow.www.fab_security.sqla.models import Permission, Resource, assoc_permission_role


def delete_dag_specific_permissions():
    with create_session() as session:
        dag_resources = session.query(Resource).filter(Resource.name.like(f"{RESOURCE_DAG_PREFIX}%")).all()
        dag_resource_ids = [d.id for d in dag_resources]

        dag_permissions = session.query(Permission).filter(Permission.resource_id.in_(dag_resource_ids)).all()
        dag_permission_ids = [d.id for d in dag_permissions]

        session.query(assoc_permission_role).filter(
            assoc_permission_role.c.permission_view_id.in_(dag_permission_ids)
        ).delete(synchronize_session=False)
        session.query(Permission).filter(Permission.resource_id.in_(dag_resource_ids)).delete(
            synchronize_session=False
        )
        session.query(Resource).filter(Resource.id.in_(dag_resource_ids)).delete(synchronize_session=False)
