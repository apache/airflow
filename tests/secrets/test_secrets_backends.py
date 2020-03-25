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
import os
import unittest

from airflow.models import Connection
from airflow.secrets.environment_variables import EnvironmentVariablesBackend
from airflow.secrets.metastore import MetastoreBackend
from airflow.utils.session import create_session


class SampleConn:
    def __init__(self, conn_id, variation: str):
        self.conn_id = conn_id
        self.var_name = "AIRFLOW_CONN_" + self.conn_id.upper()
        self.host = "host_{}.com".format(variation)
        self.conn_uri = (
            "mysql://user:pw@" + self.host + "/schema?extra1=val%2B1&extra2=val%2B2"
        )
        self.conn = Connection(conn_id=self.conn_id, uri=self.conn_uri)


class TestBaseSecretsBackend(unittest.TestCase):
    def test_env_secrets_backend(self):
        sample_conn_1 = SampleConn("sample_1", "A")
        env_secrets_backend = EnvironmentVariablesBackend()
        os.environ[sample_conn_1.var_name] = sample_conn_1.conn_uri
        conn_list = env_secrets_backend.get_connections(sample_conn_1.conn_id)
        self.assertEqual(1, len(conn_list))
        conn = conn_list[0]

        # we could make this more precise by defining __eq__ method for Connection
        self.assertEqual(sample_conn_1.host.lower(), conn.host)

    def test_metastore_secrets_backend(self):
        sample_conn_2a = SampleConn("sample_2", "A")
        sample_conn_2b = SampleConn("sample_2", "B")
        with create_session() as session:
            session.add(sample_conn_2a.conn)
            session.add(sample_conn_2b.conn)
            session.commit()
        metastore_backend = MetastoreBackend()
        conn_list = metastore_backend.get_connections("sample_2")
        host_list = {x.host for x in conn_list}
        self.assertEqual(
            {sample_conn_2a.host.lower(), sample_conn_2b.host.lower()}, set(host_list)
        )


if __name__ == "__main__":
    unittest.main()
