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

from airflow.models import Connection
from airflow.upgrade.rules.conn_id_is_unique import UniqueConnIdRule
from airflow.utils.db import create_session
from tests.test_utils.db import clear_db_connections


class TestUniqueConnIdRule(TestCase):
    def tearDown(self):
        clear_db_connections()

    def test_check(self):
        rule = UniqueConnIdRule()

        assert isinstance(rule.description, str)
        assert isinstance(rule.title, str)

        with create_session() as session:
            conn1 = Connection(conn_id="UniqueConnIdRule")
            conn2 = Connection(conn_id="UniqueConnIdRule")
            session.merge(conn1)
            session.merge(conn2)

        msgs = rule.check(session=session)
        assert [m for m in msgs if "UniqueConnIdRule" in m], \
            "UniqueConnIdRule not in warning messages"
