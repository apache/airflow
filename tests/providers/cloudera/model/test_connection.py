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

"""Tests related to the CDE connection"""

from __future__ import annotations

import json
import unittest

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from airflow.providers.cloudera.model.connection import CdeConnection


class CdeConnectionTest(unittest.TestCase):

    """Test cases for CDE connection"""

    @classmethod
    def setUpClass(cls):
        cls.engine = create_engine("sqlite:///:memory:")
        Session = sessionmaker(bind=cls.engine)
        cls.session = Session()
        CdeConnection.metadata.bind = cls.engine

    def setUp(self):
        CdeConnection.metadata.create_all()

    def tearDown(self):
        CdeConnection.metadata.drop_all(self.engine)

    def test_save_region_no_extra(self):
        """Test saving region and making sure that the extras are populated correctly"""
        cde_connection = self.create_test_connection()
        self.session.add(cde_connection)
        self.session.commit()
        cde_connection = CdeConnection.from_airflow_connection(cde_connection)
        cde_connection.save_region("eu-1", session=self.session)

        connection = self.session.query(CdeConnection).filter_by(conn_id=cde_connection.conn_id).one_or_none()
        self.assertTrue(connection is not None)
        self.assertTrue(connection.extra is not None)
        self.assertEqual(json.loads(connection.extra), {"region": "eu-1"})

    def test_save_region_with_extra(self):
        """Test saving region and making sure that the extras are not corrupted by a modification"""
        cde_connection = self.create_test_connection()
        cde_connection.set_extra(json.dumps({"foo": "bar"}))
        self.session.add(cde_connection)
        self.session.commit()
        cde_connection.save_region("ap-1", session=self.session)

        connection = self.session.query(CdeConnection).filter_by(conn_id=cde_connection.conn_id).one_or_none()
        self.assertTrue(connection is not None)
        self.assertTrue(connection.extra is not None)
        self.assertEqual(json.loads(connection.extra), {"region": "ap-1", "foo": "bar"})

    @classmethod
    def create_test_connection(cls):
        return CdeConnection(
            connection_id="id",
            host="4spn57c6.cde-lvj5dccb.dex-dev.xcu2-8y8x.dev.cldr.work",
            api_base_route="/dex/api/v1",
            scheme="https",
            access_key="access_key",
            private_key="private_key",
        )


if __name__ == "__main__":
    unittest.main()
