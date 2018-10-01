# -*- coding: utf-8 -*-
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

import unittest

from backports.configparser import DuplicateSectionError

from airflow import models
from airflow import configuration
from airflow.www import app as application
from airflow.settings import Session
from airflow.contrib.auth.backends.password_auth import PasswordUser


class ApiPasswordTests(unittest.TestCase):
    def setUp(self):
        configuration.load_test_config()
        try:
            configuration.conf.add_section("api")
        except DuplicateSectionError:
            pass

        configuration.conf.set("api",
                               "auth_backend",
                               "airflow.contrib.auth.backends.password_auth")

        self.app = application.create_app(testing=True)

        session = Session()
        user = models.User()
        password_user = PasswordUser(user)
        password_user.username = 'hello'
        password_user.password = 'world'
        session.add(password_user)
        session.commit()
        session.close()

    def test_authorized(self):
        with self.app.test_client() as c:
            response = c.get(
                '/api/experimental/pools',
                headers={'Authorization': 'Basic aGVsbG86d29ybGQ='}  # hello:world
            )
            self.assertEqual(200, response.status_code)

    def test_unauthorized(self):
        with self.app.test_client() as c:
            response = c.get('/api/experimental/pools')
            self.assertEqual(401, response.status_code)

    def tearDown(self):
        session = Session()
        session.query(models.User).delete()
        session.commit()
        session.close()
