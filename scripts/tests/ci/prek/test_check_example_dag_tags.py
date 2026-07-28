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
from check_example_dag_tags import check_file


class TestCheckFilePasses:
    """A correctly tagged example DAG produces no errors."""

    @pytest.mark.parametrize(
        "code",
        [
            pytest.param(
                'from airflow import DAG\nDAG("d", tags=["example"])\n',
                id="dag-call-with-example-tag",
            ),
            pytest.param(
                'from airflow import DAG\nwith DAG("d", tags=["example"]) as dag:\n    pass\n',
                id="dag-context-manager-with-example-tag",
            ),
            pytest.param(
                'from airflow import DAG\nDAG("d", tags=["other", "example"])\n',
                id="example-tag-among-others",
            ),
            pytest.param(
                'from airflow import DAG\nDAG("d", tags=("example",))\n',
                id="tags-as-tuple-literal",
            ),
            pytest.param(
                'from airflow import models\nmodels.DAG("d", tags=["example"])\n',
                id="dag-as-attribute",
            ),
            pytest.param(
                'from airflow.sdk import dag\n\n\n@dag(tags=["example"])\ndef my_dag():\n    pass\n',
                id="dag-decorator-with-example-tag",
            ),
            pytest.param(
                'from airflow.sdk import dag\n\n\n@dag(tags=("example",))\ndef my_dag():\n    pass\n',
                id="dag-decorator-with-tuple-tags",
            ),
            pytest.param(
                "x = 1\n",
                id="file-defines-no-dag",
            ),
            pytest.param(
                'obj.dag(tags=["other"])\n',
                id="unrelated-dag-method-call-ignored",
            ),
        ],
    )
    def test_no_errors(self, write_python_file, code: str):
        assert check_file(write_python_file(code)) == []


class TestCheckFileFails:
    """A missing or unverifiable ``example`` tag produces exactly one error."""

    @pytest.mark.parametrize(
        "code",
        [
            pytest.param(
                'from airflow import DAG\nDAG("d")\n',
                id="dag-call-without-tags",
            ),
            pytest.param(
                'from airflow import DAG\nDAG("d", tags=["other"])\n',
                id="dag-call-with-wrong-tag",
            ),
            pytest.param(
                'from airflow import DAG\nDAG("d", tags=TAGS)\n',
                id="tags-not-an-inline-literal",
            ),
            pytest.param(
                "from airflow.sdk import dag\n\n\n@dag\ndef my_dag():\n    pass\n",
                id="bare-dag-decorator",
            ),
            pytest.param(
                "from airflow.sdk import dag\n\n\n@dag()\ndef my_dag():\n    pass\n",
                id="dag-decorator-without-tags",
            ),
            pytest.param(
                'from airflow.sdk import dag\n\n\n@dag(tags=["other"])\ndef my_dag():\n    pass\n',
                id="dag-decorator-with-wrong-tag",
            ),
        ],
    )
    def test_single_error(self, write_python_file, code: str):
        assert len(check_file(write_python_file(code))) == 1

    def test_syntax_error_is_reported(self, write_python_file):
        errors = check_file(write_python_file('from airflow import DAG\nDAG("d"\n'))
        assert len(errors) == 1
        assert "could not be parsed" in errors[0]
