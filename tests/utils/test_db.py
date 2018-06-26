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

from airflow.models import Base as airflow_base

from airflow.settings import engine
from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from sqlalchemy import MetaData


class DbTest(unittest.TestCase):

    def test_database_schema_and_sqlalchemy_model_are_in_sync(self):
        all_meta_data = MetaData()
        for (table_name, table) in airflow_base.metadata.tables.items():
            all_meta_data._add_table(table_name, table.schema, table)

        # create diff between database schema and SQLAlchemy model
        mc = MigrationContext.configure(engine.connect())
        diff = compare_metadata(mc, all_meta_data)

        # known diffs to ignore
        ignores = [
            # users.password is not part of User model,
            # otherwise it would show up in (old) UI
            lambda t: (t[0] == 'remove_column' and
                       t[2] == 'users' and
                       t[3].name == 'password'),
            # ignore tables created by other tests
            lambda t: (t[0] == 'remove_table' and
                       t[1].name == 't'),
            lambda t: (t[0] == 'remove_table' and
                       t[1].name == 'test_airflow'),
            lambda t: (t[0] == 'remove_table' and
                       t[1].name == 'test_postgres_to_postgres'),
            lambda t: (t[0] == 'remove_table' and
                       t[1].name == 'test_mysql_to_mysql'),
            # ignore tables created by celery
            lambda t: (t[0] == 'remove_table' and
                       t[1].name == 'celery_taskmeta'),
            lambda t: (t[0] == 'remove_table' and
                       t[1].name == 'celery_tasksetmeta'),
            # Ignore all the fab tables
            lambda t: (t[0] == 'remove_table' and
                       t[1].name == 'ab_permission'),
            lambda t: (t[0] == 'remove_table' and
                       t[1].name == 'ab_register_user'),
            lambda t: (t[0] == 'remove_table' and
                       t[1].name == 'ab_role'),
            lambda t: (t[0] == 'remove_table' and
                       t[1].name == 'ab_permission_view'),
            lambda t: (t[0] == 'remove_table' and
                       t[1].name == 'ab_permission_view_role'),
            lambda t: (t[0] == 'remove_table' and
                       t[1].name == 'ab_user_role'),
            lambda t: (t[0] == 'remove_table' and
                       t[1].name == 'ab_user'),
            lambda t: (t[0] == 'remove_table' and
                       t[1].name == 'ab_view_menu'),
        ]
        for ignore in ignores:
            diff = [d for d in diff if not ignore(d)]

        self.assertFalse(diff, 'Database schema and SQLAlchemy model are not in sync')
