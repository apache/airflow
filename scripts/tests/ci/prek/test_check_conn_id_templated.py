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

import textwrap

import pytest
from check_conn_id_templated import build_index, check_file, main

BASE = """
    class BaseOperator:
        template_fields = ()


    class ParentOperator(BaseOperator):
        template_fields = ("sql", "parent_conn_id")

        def __init__(self, sql, parent_conn_id="parent_default", **kwargs):
            super().__init__(**kwargs)
            self.sql = sql
            self.parent_conn_id = parent_conn_id


    def aws_template_fields(*fields):
        return fields
    """


@pytest.fixture
def check(tmp_path):
    """Write the shared base classes plus the code under test, run the hook on the latter."""

    def _check(code: str) -> list[str]:
        (tmp_path / "base.py").write_text(textwrap.dedent(BASE))
        path = tmp_path / "code.py"
        path.write_text(textwrap.dedent(code))
        return check_file(path, build_index([tmp_path]))

    return _check


@pytest.mark.parametrize(
    "code",
    [
        pytest.param(
            """
            class MyOperator(BaseOperator):
                template_fields = ("bucket", "my_conn_id")

                def __init__(self, bucket, my_conn_id="my_default", **kwargs):
                    self.bucket = bucket
                    self.my_conn_id = my_conn_id
            """,
            id="listed",
        ),
        pytest.param(
            """
            class MyOperator(ParentOperator):
                def __init__(self, parent_conn_id="x", **kwargs):
                    super().__init__(parent_conn_id=parent_conn_id, **kwargs)
            """,
            id="inherits-parent-template-fields",
        ),
        pytest.param(
            """
            class MyOperator(ParentOperator):
                template_fields = (*ParentOperator.template_fields, "extra")

                def __init__(self, extra, **kwargs):
                    super().__init__(**kwargs)
                    self.extra = extra
            """,
            id="spreads-parent-template-fields",
        ),
        pytest.param(
            """
            class MyOperator(ParentOperator):
                template_fields = tuple({"extra"} | set(ParentOperator.template_fields))

                def __init__(self, extra, **kwargs):
                    super().__init__(**kwargs)
                    self.extra = extra
            """,
            id="set-union-of-parent-template-fields",
        ),
        pytest.param(
            """
            class MyOperator(BaseOperator):
                template_fields = aws_template_fields("bucket")

                def __init__(self, bucket, aws_conn_id="aws_default", **kwargs):
                    self.bucket = bucket
                    self.aws_conn_id = aws_conn_id
            """,
            id="helper-injects-aws-conn-id",
        ),
        pytest.param(
            """
            class MyOperator(BaseOperator):
                template_fields = ("bucket",)

                def __init__(self, bucket, my_conn_id="my_default", **kwargs):
                    self.bucket = bucket
                    self.my_conn_id = my_conn_id
                    self.hook = Hook(self.my_conn_id)
            """,
            id="read-in-init-cannot-be-templated",
        ),
        pytest.param(
            """
            class MyOperator(BaseOperator):
                template_fields = ("bucket",)

                def __init__(self, bucket, my_conn_id="my_default", **kwargs):
                    self.bucket = bucket
                    self._conn_id = my_conn_id
            """,
            id="not-stored-under-argument-name",
        ),
        pytest.param(
            """
            class MyTrigger(BaseOperator):
                template_fields = ()

                def __init__(self, my_conn_id, **kwargs):
                    self.my_conn_id = my_conn_id
            """,
            id="trigger-is-not-an-operator",
        ),
        pytest.param(
            """
            class MyHook:
                def __init__(self, my_conn_id, **kwargs):
                    self.my_conn_id = my_conn_id
            """,
            id="hook-is-not-an-operator",
        ),
    ],
)
def test_compliant_classes_pass(check, code):
    assert check(code) == []


@pytest.mark.parametrize(
    ("code", "expected"),
    [
        pytest.param(
            """
            class MyOperator(BaseOperator):
                template_fields = ("bucket",)

                def __init__(self, bucket, my_conn_id="my_default", **kwargs):
                    self.bucket = bucket
                    self.my_conn_id = my_conn_id
            """,
            "MyOperator: ['my_conn_id']",
            id="own-argument-missing",
        ),
        pytest.param(
            """
            class MyOperator(BaseOperator):
                def __init__(self, my_conn_id="my_default", **kwargs):
                    self.my_conn_id = my_conn_id
            """,
            "MyOperator: ['my_conn_id']",
            id="no-template-fields-anywhere",
        ),
        pytest.param(
            """
            class MyOperator(ParentOperator):
                template_fields = ("extra",)

                def __init__(self, extra, **kwargs):
                    super().__init__(**kwargs)
                    self.extra = extra
            """,
            "MyOperator: ['parent_conn_id']",
            id="redefinition-drops-parent-conn-id",
        ),
        pytest.param(
            """
            class MyOperator(ParentOperator):
                template_fields = ("extra",)

                def __init__(self, extra, parent_conn_id="x", **kwargs):
                    super().__init__(parent_conn_id=parent_conn_id, **kwargs)
                    self.extra = extra
            """,
            "MyOperator: ['parent_conn_id']",
            id="forwarded-to-parent-but-dropped",
        ),
        pytest.param(
            """
            class MySensor(BaseOperator):
                template_fields = ()

                def __init__(self, a_conn_id, b_conn_id, **kwargs):
                    self.a_conn_id = a_conn_id
                    self.b_conn_id = b_conn_id
            """,
            "MySensor: ['a_conn_id', 'b_conn_id']",
            id="several-missing-sorted",
        ),
    ],
)
def test_missing_conn_ids_are_reported(check, code, expected):
    errors = check(code)
    assert len(errors) == 1
    assert errors[0].endswith(f"{expected} missing from template_fields")


def test_main_skips_files_without_conn_id(tmp_path):
    path = tmp_path / "code.py"
    path.write_text("class MyOperator:\n    pass\n")
    assert main([str(path)], [tmp_path]) == 0


def test_main_returns_one_on_violation(tmp_path):
    path = tmp_path / "code.py"
    path.write_text(
        textwrap.dedent(
            """
            class MyOperator:
                def __init__(self, my_conn_id):
                    self.my_conn_id = my_conn_id
            """
        )
    )
    assert main([str(path)], [tmp_path]) == 1
