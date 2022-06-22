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


from flask_appbuilder.security.sqla import models as sqla_models

from airflow.security.permissions import RESOURCE_DAG_PREFIX
from airflow.utils.session import create_session


def delete_dag_specific_permissions():
    with create_session() as session:
        dag_vms = (
            session.query(sqla_models.ViewMenu)
            .filter(sqla_models.ViewMenu.name.like(f"{RESOURCE_DAG_PREFIX}%"))
            .all()
        )
        vm_ids = [d.id for d in dag_vms]

        dag_pvms = (
            session.query(sqla_models.PermissionView)
            .filter(sqla_models.PermissionView.view_menu_id.in_(vm_ids))
            .all()
        )
        pvm_ids = [d.id for d in dag_pvms]

        session.query(sqla_models.assoc_permissionview_role).filter(
            sqla_models.assoc_permissionview_role.c.permission_view_id.in_(pvm_ids)
        ).delete(synchronize_session=False)
        session.query(sqla_models.PermissionView).filter(
            sqla_models.PermissionView.view_menu_id.in_(vm_ids)
        ).delete(synchronize_session=False)
        session.query(sqla_models.ViewMenu).filter(sqla_models.ViewMenu.id.in_(vm_ids)).delete(
            synchronize_session=False
        )
