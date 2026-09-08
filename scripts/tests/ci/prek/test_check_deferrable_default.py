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

import ast

import pytest
from check_deferrable_default import _fix_invalid_deferrable_default_value

COMPAT_CONF_MODULE = "airflow.providers.common.compat.sdk"

# ``ast.unparse`` normalises to single quotes, matching ``_is_valid_deferrable_default``.
EXPECTED_DEFAULT = "conf.getboolean('operators', 'default_deferrable', fallback=False)"

INVALID_OPERATOR = """
    from __future__ import annotations


    class MyOperator:
        def __init__(self, *, deferrable: bool = False, **kwargs) -> None:
            self.deferrable = deferrable
    """


def get_conf_import_modules(code: str) -> list[str]:
    """Return the module each ``conf`` import in *code* comes from."""
    return [
        node.module or ""
        for node in ast.walk(ast.parse(code))
        if isinstance(node, ast.ImportFrom) and any(alias.name == "conf" for alias in node.names)
    ]


def get_deferrable_default(code: str) -> str | None:
    """Return the unparsed default of the keyword-only ``deferrable`` parameter in *code*."""
    for node in ast.walk(ast.parse(code)):
        if isinstance(node, ast.FunctionDef) and node.name == "__init__":
            for argument, default in zip(node.args.kwonlyargs, node.args.kw_defaults):
                if argument.arg == "deferrable" and default is not None:
                    return ast.unparse(default)
    return None


class TestFixInvalidDeferrableDefaultValue:
    def test_invalid_default_is_rewritten_with_compat_import(self, write_python_file):
        path = write_python_file(INVALID_OPERATOR)

        _fix_invalid_deferrable_default_value(str(path))

        code = path.read_text()
        assert get_deferrable_default(code) == EXPECTED_DEFAULT
        assert get_conf_import_modules(code) == [COMPAT_CONF_MODULE]

    @pytest.mark.parametrize(
        "existing_import",
        [
            pytest.param(f"from {COMPAT_CONF_MODULE} import conf", id="conf-only"),
            pytest.param(
                f"from {COMPAT_CONF_MODULE} import AirflowException, BaseOperator, conf",
                id="combined-import",
            ),
        ],
    )
    def test_existing_compat_import_is_not_shadowed(self, write_python_file, existing_import: str):
        path = write_python_file(
            f"""
            from __future__ import annotations

            {existing_import}


            class MyOperator:
                def __init__(self, *, deferrable: bool = False, **kwargs) -> None:
                    self.deferrable = deferrable
            """
        )

        _fix_invalid_deferrable_default_value(str(path))

        code = path.read_text()
        assert get_deferrable_default(code) == EXPECTED_DEFAULT
        assert get_conf_import_modules(code) == [COMPAT_CONF_MODULE]
