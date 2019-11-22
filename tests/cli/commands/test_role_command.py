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
#
import io
import unittest
from contextlib import redirect_stdout

from airflow import models
from airflow.bin import cli
from airflow.cli.commands import role_command
from airflow.settings import Session

TEST_USER1_EMAIL = 'test-user1@example.com'
TEST_USER2_EMAIL = 'test-user2@example.com'


class TestCliRoles(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dagbag = models.DagBag(include_examples=True)
        cls.parser = cli.CLIFactory.get_parser()

    def setUp(self):
        from airflow.www import app as application
        self.app, self.appbuilder = application.create_app(session=Session, testing=True)
        self.clear_roles_and_roles()

    def tearDown(self):
        self.clear_roles_and_roles()

    def clear_roles_and_roles(self):
        for email in [TEST_USER1_EMAIL, TEST_USER2_EMAIL]:
            test_user = self.appbuilder.sm.find_user(email=email)
            if test_user:
                self.appbuilder.sm.del_register_user(test_user)
        for role_name in ['FakeTeamA', 'FakeTeamB']:
            if self.appbuilder.sm.find_role(role_name):
                self.appbuilder.sm.delete_role(role_name)

    def test_cli_create_roles(self):
        self.assertIsNone(self.appbuilder.sm.find_role('FakeTeamA'))
        self.assertIsNone(self.appbuilder.sm.find_role('FakeTeamB'))

        args = self.parser.parse_args([
            'roles', 'create', 'FakeTeamA', 'FakeTeamB'
        ])
        role_command.roles_create(args)

        self.assertIsNotNone(self.appbuilder.sm.find_role('FakeTeamA'))
        self.assertIsNotNone(self.appbuilder.sm.find_role('FakeTeamB'))

    def test_cli_create_roles_is_reentrant(self):
        self.assertIsNone(self.appbuilder.sm.find_role('FakeTeamA'))
        self.assertIsNone(self.appbuilder.sm.find_role('FakeTeamB'))

        args = self.parser.parse_args([
            'roles', 'create', 'FakeTeamA', 'FakeTeamB'
        ])

        role_command.roles_create(args)

        self.assertIsNotNone(self.appbuilder.sm.find_role('FakeTeamA'))
        self.assertIsNotNone(self.appbuilder.sm.find_role('FakeTeamB'))

    def test_cli_list_roles(self):
        self.appbuilder.sm.add_role('FakeTeamA')
        self.appbuilder.sm.add_role('FakeTeamB')

        with redirect_stdout(io.StringIO()) as stdout:
            role_command.roles_list(self.parser.parse_args(['roles', 'list']))
            stdout = stdout.getvalue()

        self.assertIn('FakeTeamA', stdout)
        self.assertIn('FakeTeamB', stdout)

    def test_cli_list_roles_with_args(self):
        role_command.roles_list(self.parser.parse_args(['roles', 'list', '--output', 'tsv']))
