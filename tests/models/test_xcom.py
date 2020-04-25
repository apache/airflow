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

from airflow.models import XCom
from airflow.models.xcom import resolve_xcom_class
from tests.test_utils.config import conf_vars


class CustomXCom(XCom):
    @staticmethod
    def serialize_value(_):
        return "custom_value"


class TestXCom:
    def test_resolve_xcom_class(self):
        with conf_vars(
            {("core", "xcom_class"): "tests.models.test_xcom.CustomXCom"}
        ):
            cls = resolve_xcom_class()
            assert issubclass(cls, CustomXCom)
            assert cls().serialize_value(None) == "custom_value"
