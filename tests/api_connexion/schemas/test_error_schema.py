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
import unittest

from airflow.api_connexion.schemas.error_schema import (
    ImportErrorCollection, import_error_collection_schema, import_error_schema,
)
from airflow.models.errors import ImportError  # pylint: disable=redefined-builtin
from airflow.utils import timezone
from airflow.utils.session import provide_session
from tests.test_utils.db import clear_db_import_errors


class TestErrorSchemaBase(unittest.TestCase):
    def setUp(self) -> None:
        clear_db_import_errors()
        self.timestamp = "2020-06-10T12:02:44"

    def tearDown(self) -> None:
        clear_db_import_errors()


class TestErrorSchema(TestErrorSchemaBase):
    @provide_session
    def test_serialize(self, session):
        import_error = ImportError(
            filename="lorem.py",
            stacktrace="Lorem Ipsum",
            timestamp=timezone.parse(self.timestamp, timezone="UTC"),
        )
        session.add(import_error)
        session.commit()
        serialized_data = import_error_schema.dump(import_error)
        serialized_data["import_error_id"] = 1
        self.assertEqual(
            {
                "filename": "lorem.py",
                "import_error_id": 1,
                "stack_trace": "Lorem Ipsum",
                "timestamp": "2020-06-10T12:02:44+00:00",
            },
            serialized_data,
        )

    @unittest.skip("Not Implemented Yet")
    def test_deserialized(self, session):
        pass


class TestErrorCollectionSchema(TestErrorSchemaBase):
    @provide_session
    def test_serialize(self, session):
        import_error = [
            ImportError(
                filename="Lorem_ipsum.py",
                stacktrace="Lorem ipsum",
                timestamp=timezone.parse(self.timestamp, timezone="UTC"),
            )
            for i in range(2)
        ]
        session.add_all(import_error)
        session.commit()
        query = session.query(ImportError)
        query_list = query.all()
        serialized_data = import_error_collection_schema.dump(
            ImportErrorCollection(import_errors=query_list, total_entries=2)
        )
        # To maintain consistency in the key sequence accross the db in tests
        serialized_data["import_errors"][0]["import_error_id"] = 1
        serialized_data["import_errors"][1]["import_error_id"] = 2
        self.assertEqual(
            {
                "import_errors": [
                    {
                        "filename": "Lorem_ipsum.py",
                        "import_error_id": 1,
                        "stack_trace": "Lorem ipsum",
                        "timestamp": "2020-06-10T12:02:44+00:00",
                    },
                    {
                        "filename": "Lorem_ipsum.py",
                        "import_error_id": 2,
                        "stack_trace": "Lorem ipsum",
                        "timestamp": "2020-06-10T12:02:44+00:00",
                    },
                ],
                "total_entries": 2,
            },
            serialized_data,
        )
