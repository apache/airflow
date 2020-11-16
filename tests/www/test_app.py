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
from datetime import timedelta

import pytest
from werkzeug.middleware.proxy_fix import ProxyFix

from airflow.settings import Session
from airflow.www_rbac import app as application
from tests.compat import mock
from tests.test_utils.config import conf_vars


class TestApp(unittest.TestCase):
    @conf_vars({('webserver', 'enable_proxy_fix'): 'False'})
    def test_constructor_no_proxyfix(self):
        app, _ = application.create_app(testing=True)
        self.assertFalse(isinstance(app.wsgi_app, ProxyFix))

    @conf_vars({('webserver', 'enable_proxy_fix'): 'True'})
    def test_constructor_proxyfix(self):
        app, _ = application.create_app(testing=True)
        self.assertTrue(isinstance(app.wsgi_app, ProxyFix))
        self.assertEqual(app.wsgi_app.x_for, 1)
        self.assertEqual(app.wsgi_app.x_proto, 1)
        self.assertEqual(app.wsgi_app.x_host, 1)
        self.assertEqual(app.wsgi_app.x_port, 1)
        self.assertEqual(app.wsgi_app.x_prefix, 1)

    @conf_vars({('webserver', 'enable_proxy_fix'): 'True',
                ('webserver', 'proxy_fix_x_for'): '3',
                ('webserver', 'proxy_fix_x_proto'): '4',
                ('webserver', 'proxy_fix_x_host'): '5',
                ('webserver', 'proxy_fix_x_port'): '6',
                ('webserver', 'proxy_fix_x_prefix'): '7'})
    def test_constructor_proxyfix_args(self):
        app, _ = application.create_app(testing=True)
        self.assertTrue(isinstance(app.wsgi_app, ProxyFix))
        self.assertEqual(app.wsgi_app.x_for, 3)
        self.assertEqual(app.wsgi_app.x_proto, 4)
        self.assertEqual(app.wsgi_app.x_host, 5)
        self.assertEqual(app.wsgi_app.x_port, 6)
        self.assertEqual(app.wsgi_app.x_prefix, 7)

    @conf_vars({
        ('core', 'sql_alchemy_pool_enabled'): 'True',
        ('core', 'sql_alchemy_pool_size'): '3',
        ('core', 'sql_alchemy_max_overflow'): '5',
        ('core', 'sql_alchemy_pool_recycle'): '120',
        ('core', 'sql_alchemy_pool_pre_ping'): 'True',
    })
    @mock.patch("airflow.www.app.app", None)
    @pytest.mark.backend("mysql", "postgres")
    def test_should_set_sqlalchemy_engine_options(self):
        app, _ = application.create_app(session=Session, testing=True)
        engine_params = {
            'pool_size': 3,
            'pool_recycle': 120,
            'pool_pre_ping': True,
            'max_overflow': 5
        }
        self.assertEqual(app.config['SQLALCHEMY_ENGINE_OPTIONS'], engine_params)

    @conf_vars(
        {
            ('webserver', 'session_lifetime_minutes'): '3600',
        }
    )
    @mock.patch("airflow.www.app.app", None)
    def test_should_set_permanent_session_timeout(self):
        app, _ = application.create_app(session=Session, testing=True)
        self.assertEqual(app.config['PERMANENT_SESSION_LIFETIME'], timedelta(minutes=3600))
