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

from flask_appbuilder import AppBuilder

from airflow import settings
from airflow.configuration import conf


def init_appbuilder(app):
    """Init `Flask App Builder <https://flask-appbuilder.readthedocs.io/en/latest/>`__."""
    from airflow.www.security import AirflowSecurityManager

    security_manager_class = app.config.get('SECURITY_MANAGER_CLASS') or AirflowSecurityManager

    if not issubclass(security_manager_class, AirflowSecurityManager):
        raise Exception(
            """Your CUSTOM_SECURITY_MANAGER must now extend AirflowSecurityManager,
             not FAB's security manager."""
        )

    class AirflowAppBuilder(AppBuilder):
        """
        Custom class to prevent side effects of the session.
        """

        def _check_and_init(self, baseview):
            if hasattr(baseview, 'datamodel'):
                # Delete sessions if initiated previously to limit side effects. We want to use
                # the current session in the current application.
                baseview.datamodel.session = None
            return super()._check_and_init(baseview)

    AirflowAppBuilder(
        app=app,
        session=settings.Session,
        security_manager_class=security_manager_class,
        base_template='airflow/master.html',
        update_perms=conf.getboolean('webserver', 'UPDATE_FAB_PERMS'),
    )
