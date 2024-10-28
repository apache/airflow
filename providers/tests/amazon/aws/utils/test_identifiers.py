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

import random
import string
import uuid

import pytest

from airflow.providers.amazon.aws.utils.identifiers import generate_uuid
from airflow.utils.types import NOTSET


class TestGenerateUuid:
    @pytest.fixture(
        autouse=True,
        params=[
            pytest.param(NOTSET, id="default-namespace"),
            pytest.param(uuid.UUID(int=42), id="custom-namespace"),
        ],
    )
    def setup_namespace(self, request):
        self.default_namespace = request.param is NOTSET
        self.namespace = uuid.NAMESPACE_OID if self.default_namespace else request.param
        self.kwargs = {"namespace": self.namespace} if not self.default_namespace else {}

    def test_deterministic(self):
        """Test that result is deterministic and a valid UUID object"""
        args = [
            "".join(random.choices(string.ascii_letters, k=random.randint(3, 13)))
            for _ in range(100)
        ]
        result = generate_uuid(*args, **self.kwargs)
        assert result == generate_uuid(*args, **self.kwargs)
        assert uuid.UUID(result).version == 5, "Should generate UUID v5"

    def test_nil_uuid(self):
        """Test that result of single None are NIL UUID, regardless namespace."""
        assert (
            generate_uuid(None, **self.kwargs) == "00000000-0000-0000-0000-000000000000"
        )

    def test_single_uuid_value(self):
        """Test that result of single not None value are the same as uuid5."""
        assert generate_uuid("", **self.kwargs) == str(uuid.uuid5(self.namespace, ""))
        assert generate_uuid("Airflow", **self.kwargs) == str(
            uuid.uuid5(self.namespace, "Airflow")
        )

    def test_multiple_none_value(self):
        """Test that result of single None are NIL UUID, regardless of namespace."""
        multi_none = generate_uuid(None, None, **self.kwargs)
        assert multi_none != "00000000-0000-0000-0000-000000000000"
        assert uuid.UUID(multi_none).version == 5

        # Test that None values not skipped
        assert generate_uuid(None, "1", None, **self.kwargs) != generate_uuid(
            "1", **self.kwargs
        )
        assert generate_uuid(None, "1", **self.kwargs) != generate_uuid(
            "1", **self.kwargs
        )
        assert generate_uuid("1", None, **self.kwargs) != generate_uuid(
            "1", **self.kwargs
        )

    def test_no_args_value(self):
        with pytest.raises(ValueError, match="Expected at least 1 argument"):
            generate_uuid(**self.kwargs)
