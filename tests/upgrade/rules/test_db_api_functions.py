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
from unittest import TestCase

from airflow.hooks.base_hook import BaseHook
from airflow.hooks.dbapi_hook import DbApiHook
from airflow.upgrade.rules.db_api_functions import DbApiRule


class MyHook(BaseHook):
    def get_records(self, sql):
        pass

    def run(self, sql):
        pass

    def get_pandas_df(self, sql):
        pass

    def get_conn(self):
        pass


class ProperDbApiHook(DbApiHook):

    def bulk_dump(self, table, tmp_file):
        pass

    def bulk_load(self, table, tmp_file):
        pass

    def get_records(self, sql, *kwargs):
        pass

    def run(self, sql, *kwargs):
        pass

    def get_pandas_df(self, sql, *kwargs):
        pass


class TestSqlHookCheck(TestCase):
    def test_fails_on_incorrect_hook(self):
        db_api_rule_failures = DbApiRule().check()
        self.assertEqual(len(db_api_rule_failures), 3)
        proper_db_api_hook_failures = \
            [failure for failure in db_api_rule_failures if "ProperDbApiHook" in failure]
        self.assertEqual(len(proper_db_api_hook_failures), 0)
