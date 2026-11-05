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
from __future__ import annotations

from unittest.mock import patch

import pytest

from airflow.providers.sas.dialects.sas import SASDialect
from airflow.providers.sas.hooks.sas import SASHook

from unit.sas.fakes import _sas_connection


def test_dialect_binds_values_and_validates_identifiers():
    hook = SASHook(sas_conn_id="sas.test", connection=_sas_connection())
    dialect = SASDialect(hook)
    with patch.object(SASHook, "get_records", return_value=[("ID", "num", "no", 1)]) as get_records:
        assert dialect.get_column_names("MYLIB.ORDERS") == ["ID"]

    query = get_records.call_args.kwargs["sql"]
    assert "FROM MYLIB.ORDERS(obs=0)" in query
    assert "WHERE LIBNAME=%s AND MEMNAME=%s" in query
    assert get_records.call_args.kwargs["parameters"] == ("MYLIB", "ORDERS")

    for table in ("ORDERS", "MYLIB.T; drop table x"):
        with pytest.raises(ValueError, match=r"<library>\.<member>"):
            dialect.get_column_names(table)
