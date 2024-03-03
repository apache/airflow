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

from airflow.providers.amazon.aws.utils.mixins import aws_template_fields


def test_aws_template_fields():
    result = aws_template_fields()
    assert result == ("aws_conn_id", "region_name", "verify")
    assert aws_template_fields() is result, "aws_template_fields expect to return cached result"

    assert aws_template_fields("foo", "aws_conn_id", "bar") == (
        "aws_conn_id",
        "bar",
        "foo",
        "region_name",
        "verify",
    )


def test_aws_template_fields_incorrect_types():
    with pytest.raises(TypeError, match="Expected that all provided arguments are strings"):
        aws_template_fields(1, None, "test")
