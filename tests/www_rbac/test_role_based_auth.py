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

import json
import unittest
from datetime import datetime

from flask import g

from airflow import settings
from airflow.api.common.experimental import delete_dag, pool
from airflow.exceptions import DagNotFound, PoolNotFound
from airflow.models.dag import DagModel
from airflow.settings import Session
from airflow.www_rbac import app as application
from tests.test_utils.config import conf_vars


class RoleBasedAuthTest(unittest.TestCase):
    def setUp(self):
        self.tearDownClass()
        with conf_vars(
            {("api", "auth_backend"): "airflow.api.auth.backend.role_based_auth"}
        ):
            self.tearDownClass()
            self.app, self.appbuilder = application.create_app(
                session=Session, testing=True
            )
            self.app.config["WTF_CSRF_ENABLED"] = False
            settings.configure_orm()
            self.session = Session
            self.admin_role = self.appbuilder.sm.find_role(
                "Admin"
            )  # pylint: disable=no-member
            admin_username = "das_admin"
            admin_email = "das_admin@fab.org"
            self.admin_user = self.appbuilder.sm.find_user(  # pylint: disable=no-member
                username=admin_username, email=admin_email
            )
            if not self.admin_user:
                self.admin_user = self.appbuilder.sm.add_user(  # pylint: disable=no-member
                    admin_username,
                    "admin",
                    "user",
                    admin_email,
                    self.admin_role,
                    "general",
                )
            self.admin_user.roles = [self.admin_role]

            viewer_username = "das_viewer"
            viewer_email = "das_viewer@fab.org"
            self.viewer_role = self.appbuilder.sm.find_role(
                "Public"
            )  # pylint: disable=no-member
            self.viewer_user = self.appbuilder.sm.find_user(  # pylint: disable=no-member
                username=viewer_username, email=viewer_email
            )
            if not self.viewer_user:
                self.viewer_user = self.appbuilder.sm.add_user(  # pylint: disable=no-member
                    viewer_username,
                    "viewer",
                    "user",
                    viewer_email,
                    self.viewer_role,
                    "general",
                )
            self.viewer_user.roles = [self.viewer_role]

    @classmethod
    def tearDownClass(cls):
        pools = [
            "test_delete_pool_unauthorized",
            "test_delete_pool_authorized",
            "test_create_pool_unauthorized",
            "test_create_pool_authorized",
        ]
        for pool_name in pools:
            try:
                pool.delete_pool(pool_name)
            except PoolNotFound:
                pass

        dags = [
            "test_pause_dag_unauthorized",
            "test_pause_dag_unauthorized",
            "test_pause_dag_authorized",
            "test_trigger_dag_unauthorized",
        ]

        for dag in dags:
            try:
                delete_dag.delete_dag(dag)
            except DagNotFound:
                pass

    def create_test_dag(self, dag_id):
        dag = DagModel(dag_id=dag_id)
        self.session.add(dag)
        self.session.commit()

    def use_admin_role(self):
        @self.app.before_request
        def before_request():  # pylint: disable=unused-variable
            g.user = self.admin_user

    def use_viewer_role(self):
        @self.app.before_request
        def before_request():  # pylint: disable=unused-variable
            g.user = self.viewer_user

    def test_trigger_dag_authorized(self):
        self.use_admin_role()
        with self.app.test_client() as client:
            dag_id = "test_example_bash_operator"
            response = client.post(
                "/api/experimental/dags/{}/dag_runs".format(dag_id),
                data=json.dumps(dict(run_id="my_run" + datetime.now().isoformat())),
                content_type="application/json",
            )

            self.assertEqual(200, response.status_code)

    def test_trigger_dag_unauthorized(self):
        dag_id = "test_trigger_dag_unauthorized"
        self.create_test_dag(dag_id)

        self.use_viewer_role()
        with self.app.test_client() as client:
            response = client.post(
                "/api/experimental/dags/{}/dag_runs".format(dag_id),
                data=json.dumps(dict(run_id="my_run" + datetime.now().isoformat())),
                content_type="application/json",
            )

            self.assertEqual(403, response.status_code)

    def test_pause_dag_authorized(self):
        self.use_admin_role()
        with self.app.test_client() as client:
            dag_id = "test_example_bash_operator"
            response = client.get("/api/experimental/dags/{}/paused/true".format(dag_id))

            self.assertEqual(200, response.status_code)

    def test_pause_dag_unauthorized(self):
        dag_id = "test_pause_dag_unauthorized"
        self.create_test_dag(dag_id)

        self.use_viewer_role()
        with self.app.test_client() as client:
            response = client.get("/api/experimental/dags/{}/paused/true".format(dag_id))
            self.assertEqual(403, response.status_code)

    def test_create_pool_authorized(self):
        self.use_admin_role()
        with self.app.test_client() as client:
            response = client.post(
                "/api/experimental/pools",
                data=json.dumps(
                    dict(
                        name="test_create_pool_authorized",
                        slots=32,
                        description="a description",
                    )
                ),
                content_type="application/json",
            )

            self.assertEqual(200, response.status_code)

    def test_create_pool_unauthorized(self):
        self.use_viewer_role()
        with self.app.test_client() as client:
            response = client.post(
                "/api/experimental/pools",
                data=json.dumps(
                    dict(
                        name="test_create_pool_unauthorized",
                        slots=32,
                        description="a description",
                    )
                ),
                content_type="application/json",
            )

            self.assertEqual(403, response.status_code)

    def test_delete_pool_authorized(self):
        pool_name = "test_delete_pool_authorized"
        pool.create_pool(pool_name, 16, "a description")
        self.use_admin_role()
        with self.app.test_client() as client:
            response = client.delete("/api/experimental/pools/{}".format(pool_name))

            self.assertEqual(200, response.status_code)

    def test_delete_pool_unauthorized(self):
        pool_name = "test_delete_pool_unauthorized"
        pool.create_pool(pool_name, 16, "a description")
        self.use_viewer_role()
        with self.app.test_client() as client:
            response = client.delete("/api/experimental/pools/{}".format(pool_name))

            self.assertEqual(403, response.status_code)
