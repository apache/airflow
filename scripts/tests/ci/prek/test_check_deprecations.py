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

from ci.prek.check_deprecations import (
    built_import,
    built_import_from,
    found_compatible_decorators,
    get_decorator_argument,
    is_file_under_eol_deprecation,
    resolve_decorator_name,
    resolve_name,
)

GOOGLE_BIGQUERY_HOOK_PATH = "airflow/providers/google/cloud/hooks/bigquery.py"
AIRFLOW_PROVIDER_DEPRECATION_WARNING = "AirflowProviderDeprecationWarning"


class TestResolveName:
    def test_simple_name(self):
        expr = ast.parse("foo", mode="eval").body
        assert resolve_name(expr) == "foo"

    def test_attribute(self):
        expr = ast.parse("foo.bar", mode="eval").body
        assert resolve_name(expr) == "foo.bar"

    def test_nested_attribute(self):
        expr = ast.parse("foo.bar.baz", mode="eval").body
        assert resolve_name(expr) == "foo.bar.baz"


class TestResolveDecoratorName:
    def test_name_decorator(self):
        func = ast.parse("@deprecated\ndef foo(): pass").body[0]
        assert resolve_decorator_name(func.decorator_list[0]) == "deprecated"

    def test_call_decorator(self):
        func = ast.parse("@deprecated(category=X)\ndef foo(): pass").body[0]
        assert resolve_decorator_name(func.decorator_list[0]) == "deprecated"

    def test_attribute_decorator(self):
        func = ast.parse("@warnings.deprecated\ndef foo(): pass").body[0]
        assert resolve_decorator_name(func.decorator_list[0]) == "warnings.deprecated"

    def test_attribute_call_decorator(self):
        func = ast.parse("@warnings.deprecated(category=X)\ndef foo(): pass").body[0]
        assert resolve_decorator_name(func.decorator_list[0]) == "warnings.deprecated"


class TestBuiltImportFrom:
    def test_from_warnings_import_deprecated(self):
        node = ast.parse("from warnings import deprecated").body[0]
        result = built_import_from(node)
        assert "deprecated" in result

    def test_from_typing_extensions_import_deprecated(self):
        node = ast.parse("from typing_extensions import deprecated").body[0]
        result = built_import_from(node)
        assert "deprecated" in result

    def test_from_deprecated_import_deprecated(self):
        node = ast.parse("from deprecated import deprecated").body[0]
        result = built_import_from(node)
        assert "deprecated" in result

    def test_from_deprecated_classic_import_deprecated(self):
        node = ast.parse("from deprecated.classic import deprecated").body[0]
        result = built_import_from(node)
        assert "deprecated" in result

    def test_unrelated_import(self):
        node = ast.parse("from os import path").body[0]
        result = built_import_from(node)
        assert result == []

    def test_aliased_import(self):
        node = ast.parse("from warnings import deprecated as dep").body[0]
        result = built_import_from(node)
        assert "dep" in result

    def test_no_module_name(self):
        # relative import with no module
        node = ast.parse("from . import something").body[0]
        result = built_import_from(node)
        assert result == []

    def test_import_parent_module(self):
        node = ast.parse("from deprecated import classic").body[0]
        result = built_import_from(node)
        assert "classic.deprecated" in result


class TestBuiltImport:
    def test_import_warnings(self):
        node = ast.parse("import warnings").body[0]
        result = built_import(node)
        assert "warnings.deprecated" in result

    def test_import_typing_extensions(self):
        node = ast.parse("import typing_extensions").body[0]
        result = built_import(node)
        assert "typing_extensions.deprecated" in result

    def test_import_deprecated(self):
        node = ast.parse("import deprecated").body[0]
        result = built_import(node)
        assert "deprecated.deprecated" in result

    def test_import_unrelated(self):
        node = ast.parse("import os").body[0]
        result = built_import(node)
        assert result == []

    def test_import_with_alias(self):
        node = ast.parse("import warnings as w").body[0]
        result = built_import(node)
        assert "w.deprecated" in result


class TestFoundCompatibleDecorators:
    def test_no_imports(self):
        mod = ast.parse("x = 1")
        assert found_compatible_decorators(mod) == ()

    def test_with_warnings_import(self):
        mod = ast.parse("from warnings import deprecated")
        result = found_compatible_decorators(mod)
        assert "deprecated" in result

    def test_with_multiple_imports(self):
        code = "from warnings import deprecated\nfrom deprecated import deprecated as dep"
        mod = ast.parse(code)
        result = found_compatible_decorators(mod)
        assert "dep" in result
        assert "deprecated" in result

    def test_deduplication(self):
        code = "from warnings import deprecated\nfrom typing_extensions import deprecated"
        mod = ast.parse(code)
        result = found_compatible_decorators(mod)
        assert result.count("deprecated") == 1


class TestGetDecoratorArgument:
    def test_finds_keyword(self):
        func = ast.parse("@deprecated(category=DeprecationWarning)\ndef foo(): pass").body[0]
        decorator = func.decorator_list[0]
        result = get_decorator_argument(decorator, "category")
        assert result is not None
        assert result.arg == "category"

    def test_missing_keyword(self):
        func = ast.parse("@deprecated(message='old')\ndef foo(): pass").body[0]
        decorator = func.decorator_list[0]
        result = get_decorator_argument(decorator, "category")
        assert result is None

    def test_multiple_keywords(self):
        code = "@deprecated(message='old', category=DeprecationWarning)\ndef foo(): pass"
        func = ast.parse(code).body[0]
        decorator = func.decorator_list[0]
        result = get_decorator_argument(decorator, "category")
        assert result is not None


class TestIsFileUnderEolDeprecation:
    def test_google_provider_with_matching_warning(self):
        assert is_file_under_eol_deprecation(
            GOOGLE_BIGQUERY_HOOK_PATH,
            AIRFLOW_PROVIDER_DEPRECATION_WARNING,
        )

    def test_google_provider_with_non_matching_warning(self):
        assert not is_file_under_eol_deprecation(
            GOOGLE_BIGQUERY_HOOK_PATH,
            "DeprecationWarning",
        )

    def test_non_google_provider(self):
        assert not is_file_under_eol_deprecation(
            "airflow/providers/amazon/aws/hooks/s3.py",
            AIRFLOW_PROVIDER_DEPRECATION_WARNING,
        )

    def test_core_airflow_file(self):
        assert not is_file_under_eol_deprecation(
            "airflow/models/dag.py",
            AIRFLOW_PROVIDER_DEPRECATION_WARNING,
        )
