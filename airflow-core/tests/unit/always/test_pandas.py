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
from __future__ import annotations

import pytest
from sqlalchemy import create_engine

from airflow.configuration import conf

pd = pytest.importorskip("pandas")
pyarrow = pytest.importorskip("pyarrow")

pytestmark = pytest.mark.db_test


@pytest.mark.backend("postgres", "mysql")
class TestPandasSQLAlchemyCompatibility:
    @pytest.fixture(autouse=True)
    def _setup_test_cases(self):
        self.temp_table = "test_to_pandas"
        uri = conf.get_mandatory_value("database", "sql_alchemy_conn")
        self.engine = create_engine(uri)
        yield
        if self.engine:
            with self.engine.begin() as conn:
                conn.execute(f"DROP TABLE IF EXISTS {self.temp_table};")
            self.engine.dispose()

    def test_write_read_to_db(self):
        pd.DataFrame({"a": [0, 1, 2, 3]}).to_sql(self.temp_table, self.engine)
        pd.read_sql(f"SELECT * FROM {self.temp_table}", self.engine)


class TestPandasSQLAlchemyCompatibilitySQLite:
    @pytest.fixture(autouse=True)
    def _setup_test_cases(self):
        self.temp_table = "test_to_pandas"
        self.engine = create_engine("sqlite:///:memory:")
        yield
        if self.engine:
            self.engine.dispose()

    def test_write_read_to_db(self):
        pd.DataFrame({"a": [0, 1, 2, 3]}).to_sql(self.temp_table, self.engine)
        pd.read_sql(f"SELECT * FROM {self.temp_table}", self.engine)
