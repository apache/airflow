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

import locale
from base64 import b64decode, b64encode
from datetime import datetime
from uuid import uuid4

import pendulum

from airflow.providers.microsoft.azure.serialization.serializer import ResponseSerializer
from tests.providers.microsoft.conftest import load_file, load_json


class TestResponseSerializer:
    def test_serialize_when_bytes_then_base64_encoded(self):
        response = load_file("resources", "dummy.pdf", mode="rb", encoding=None)
        content = b64encode(response).decode(locale.getpreferredencoding())

        actual = ResponseSerializer().serialize(response)

        assert isinstance(actual, str)
        assert actual == content

    def test_serialize_when_dict_with_uuid_datatime_and_pendulum_then_json(self):
        id = uuid4()
        response = {
            "id": id,
            "creationDate": datetime(2024, 2, 5),
            "modificationTime": pendulum.datetime(2024, 2, 5),
        }

        actual = ResponseSerializer().serialize(response)

        assert isinstance(actual, str)
        assert (
            actual
            == f'{{"id": "{id}", "creationDate": "2024-02-05T00:00:00", "modificationTime": "2024-02-05T00:00:00+00:00"}}'
        )

    def test_deserialize_when_json(self):
        response = load_file("resources", "users.json")

        actual = ResponseSerializer().deserialize(response)

        assert isinstance(actual, dict)
        assert actual == load_json("resources", "users.json")

    def test_deserialize_when_base64_encoded_string(self):
        content = load_file("resources", "dummy.pdf", mode="rb", encoding=None)
        response = b64encode(content).decode(locale.getpreferredencoding())

        actual = ResponseSerializer().deserialize(response)

        assert actual == response
        assert b64decode(actual) == content
