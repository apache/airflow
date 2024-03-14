import locale
from base64 import b64encode, b64decode
from datetime import datetime
from uuid import uuid4

import pendulum
from airflow.providers.microsoft.msgraph.serialization.serializer import ResponseSerializer

from tests.unit.conftest import load_json, load_file


class TestResponseSerializer:
    def test_serialize_when_bytes_then_base64_encoded(self):
        response = load_file("resources", "dummy.pdf", mode="rb", encoding=None)
        content = b64encode(response).decode(locale.getpreferredencoding())

        actual = ResponseSerializer().serialize(response)

        assert isinstance(actual, str)
        assert actual == content

    def test_serialize_when_dict_with_uuid_datatime_and_pendulum_then_json(self):
        id = uuid4()
        response = {"id": id, "creationDate": datetime(2024, 2, 5), "modificationTime": pendulum.datetime(2024, 2, 5)}

        actual = ResponseSerializer().serialize(response)

        assert isinstance(actual, str)
        assert actual == f'{{"id": "{id}", "creationDate": "2024-02-05T00:00:00", "modificationTime": "2024-02-05T00:00:00+00:00"}}'

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
