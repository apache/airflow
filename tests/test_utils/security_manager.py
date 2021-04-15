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


from flask_appbuilder.security.sqla.models import assoc_permissionview_role

from airflow.security.permissions import RESOURCE_DAG_PREFIX
from airflow.utils.session import create_session
from airflow.www.security import SimpleAirflowSecurityManager


def delete_dag_specific_permissions():
    with create_session() as session:
        security_manager = SimpleAirflowSecurityManager(session=session)

        dag_vms = (
            session.query(security_manager.viewmenu_model)
            .filter(security_manager.viewmenu_model.name.like(f"{RESOURCE_DAG_PREFIX}%"))
            .all()
        )
        vm_ids = [d.id for d in dag_vms]

        dag_pvms = (
            session.query(security_manager.permissionview_model)
            .filter(security_manager.permissionview_model.view_menu_id.in_(vm_ids))
            .all()
        )
        pvm_ids = [d.id for d in dag_pvms]

        session.query(assoc_permissionview_role).filter(
            assoc_permissionview_role.c.permission_view_id.in_(pvm_ids)
        ).delete(synchronize_session=False)
        session.query(security_manager.permissionview_model).filter(
            security_manager.permissionview_model.view_menu_id.in_(vm_ids)
        ).delete(synchronize_session=False)
        session.query(security_manager.viewmenu_model).filter(
            security_manager.viewmenu_model.id.in_(vm_ids)
        ).delete(synchronize_session=False)
