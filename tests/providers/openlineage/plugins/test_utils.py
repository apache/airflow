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

# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import datetime
import json
import os
import uuid
from json import JSONEncoder
from typing import Any

from attrs import define
from openlineage.client.utils import RedactMixin
from pendulum.tz.timezone import Timezone
from pkg_resources import parse_version

from airflow.models import DAG as AIRFLOW_DAG, DagModel
from airflow.operators.empty import EmptyOperator
from airflow.providers.openlineage.utils.utils import (
    InfoJsonEncodable,
    OpenLineageRedactor,
    _is_name_redactable,
    get_connection,
    to_json_encodable,
    url_to_https,
)
from airflow.utils.log.secrets_masker import _secrets_masker
from airflow.utils.state import State

AIRFLOW_CONN_ID = "test_db"
AIRFLOW_CONN_URI = "postgres://localhost:5432/testdb"
SNOWFLAKE_CONN_URI = "snowflake://12345.us-east-1.snowflakecomputing.com/MyTestRole?extra__snowflake__account=12345&extra__snowflake__database=TEST_DB&extra__snowflake__insecure_mode=false&extra__snowflake__region=us-east-1&extra__snowflake__role=MyTestRole&extra__snowflake__warehouse=TEST_WH&extra__snowflake__aws_access_key_id=123456&extra__snowflake__aws_secret_access_key=abcdefg"  # NOQA


class SafeStrDict(dict):
    def __str__(self):
        castable = []
        for key, val in self.items():
            try:
                str(key), str(val)
                castable.append((key, val))
            except (TypeError, NotImplementedError):
                continue
        return str(dict(castable))


def test_get_connection():
    os.environ["AIRFLOW_CONN_DEFAULT"] = AIRFLOW_CONN_URI

    conn = get_connection("default")
    assert conn.host == "localhost"
    assert conn.port == 5432
    assert conn.conn_type == "postgres"
    assert conn


def test_url_to_https_no_url():
    assert url_to_https(None) is None
    assert url_to_https("") is None


def test_get_dagrun_start_end():
    start_date = datetime.datetime(2022, 1, 1)
    end_date = datetime.datetime(2022, 1, 1, hour=2)
    dag = AIRFLOW_DAG("test", start_date=start_date, end_date=end_date, schedule_interval="@once")
    AIRFLOW_DAG.bulk_write_to_db([dag])
    dag_model = DagModel.get_dagmodel(dag.dag_id)
    run_id = str(uuid.uuid1())
    dagrun = dag.create_dagrun(
        state=State.NONE, run_id=run_id, data_interval=dag.get_next_data_interval(dag_model)
    )
    assert dagrun.data_interval_start is not None
    start_date_tz = datetime.datetime(2022, 1, 1, tzinfo=Timezone("UTC"))
    end_date_tz = datetime.datetime(2022, 1, 1, hour=2, tzinfo=Timezone("UTC"))
    assert dagrun.data_interval_start, dagrun.data_interval_end == (start_date_tz, end_date_tz)


def test_parse_version():
    assert parse_version("2.3.0") >= parse_version("2.3.0.dev0")
    assert parse_version("2.3.0.dev0") >= parse_version("2.3.0.dev0")
    assert parse_version("2.3.0.beta1") >= parse_version("2.3.0.dev0")
    assert parse_version("2.3.1") >= parse_version("2.3.0.dev0")
    assert parse_version("2.4.0") >= parse_version("2.3.0.dev0")
    assert parse_version("3.0.0") >= parse_version("2.3.0.dev0")
    assert parse_version("2.2.0") < parse_version("2.3.0.dev0")
    assert parse_version("2.1.3") < parse_version("2.3.0.dev0")
    assert parse_version("2.2.4") < parse_version("2.3.0.dev0")
    assert parse_version("1.10.15") < parse_version("2.3.0.dev0")
    assert parse_version("2.2.4.dev0") < parse_version("2.3.0.dev0")


def test_to_json_encodable():
    dag = AIRFLOW_DAG(
        dag_id="test_dag", schedule_interval="*/2 * * * *", start_date=datetime.datetime.now(), catchup=False
    )
    task = EmptyOperator(task_id="test_task", dag=dag)

    encodable = to_json_encodable(task)
    encoded = json.dumps(encodable)
    decoded = json.loads(encoded)
    assert decoded == encodable


def test_safe_dict():
    assert str(SafeStrDict({"a": 1})) == str({"a": 1})

    class NotImplemented:
        def __str__(self):
            raise NotImplementedError

    assert str(SafeStrDict({"a": NotImplemented()})) == str({})


def test_info_json_encodable():
    class TestInfo(InfoJsonEncodable):
        excludes = ["exclude_1", "exclude_2", "imastring"]
        casts = {"iwanttobeint": lambda x: int(x.imastring)}
        renames = {"_faulty_name": "goody_name"}

    @define(slots=False)
    class Test:
        exclude_1: str
        imastring: str
        _faulty_name: str
        donotcare: str

    obj = Test("val", "123", "not_funny", "abc")

    assert json.loads(json.dumps(TestInfo(obj))) == {
        "iwanttobeint": 123,
        "goody_name": "not_funny",
        "donotcare": "abc",
    }


def test_is_name_redactable():
    class NotMixin:
        def __init__(self):
            self.password = "passwd"

    class Mixined(RedactMixin):
        _skip_redact = ["password"]

        def __init__(self):
            self.password = "passwd"
            self.transparent = "123"

    assert _is_name_redactable("password", NotMixin())
    assert not _is_name_redactable("password", Mixined())
    assert _is_name_redactable("transparent", Mixined())


def test_redact_with_exclusions(monkeypatch):
    redactor = OpenLineageRedactor.from_masker(_secrets_masker())

    class NotMixin:
        def __init__(self):
            self.password = "passwd"

    def default(self, o):
        if isinstance(o, NotMixin):
            return o.__dict__
        raise TypeError

    assert redactor.redact(NotMixin()).password == "passwd"
    monkeypatch.setattr(JSONEncoder, "default", default)
    assert redactor.redact(NotMixin()).password == "***"

    class Mixined(RedactMixin):
        _skip_redact = ["password"]

        def __init__(self):
            self.password = "passwd"
            self.transparent = "123"

    @define(slots=False)
    class NestedMixined(RedactMixin):
        _skip_redact = ["nested_field"]
        password: str
        nested_field: Any

    assert redactor.redact(Mixined()).password == "passwd"
    assert redactor.redact(Mixined()).transparent == "123"
    assert redactor.redact({"password": "passwd"}) == {"password": "***"}
    redacted_nested = redactor.redact(NestedMixined("passwd", NestedMixined("passwd", None)))
    assert redacted_nested == NestedMixined("***", NestedMixined("passwd", None))
