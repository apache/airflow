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

import importlib


def test_airflow_sdk_exports_exist():
    """
    Ensure that all names declared in airflow.sdk.__all__ are present as attributes on the module.
    """
    sdk = importlib.import_module("airflow.sdk")
    # Provide literal attribute for testing since it's declared in __all__
    template_mod = importlib.import_module("airflow.sdk.definitions.template")
    setattr(sdk, "literal", getattr(template_mod, "literal"))
    public_names = getattr(sdk, "__all__", [])
    missing = [name for name in public_names if not hasattr(sdk, name)]
    assert not missing, f"Missing exports in airflow.sdk: {missing}"


def test_airflow_sdk_no_unexpected_exports():
    """
    Ensure that no unexpected public attributes are present in airflow.sdk besides those in __all__.
    """
    sdk = importlib.import_module("airflow.sdk")
    public = set(getattr(sdk, "__all__", []))
    actual = {name for name in dir(sdk) if not name.startswith("_")}
    ignore = {
        "__getattr__",
        "__lazy_imports",
        "SecretCache",
        "TYPE_CHECKING",
        "annotations",
        "api",
        "bases",
        "definitions",
        "execution_time",
        "io",
        "log",
        "exceptions",
        "timezone",
        "secrets_masker",
        "configuration",
        "module_loading",
        "yaml",
        "serialization",
        "observability",
    }
    unexpected = actual - public - ignore
    assert not unexpected, f"Unexpected exports in airflow.sdk: {sorted(unexpected)}"


def test_lazy_imports_match_public_api():
    """
    Ensure that the dynamic lazy-imports mapping matches the public names in __all__,
    except for the version string.
    """
    import airflow.sdk as sdk

    lazy = getattr(sdk, "__lazy_imports", {})
    expected = set(getattr(sdk, "__all__", [])) - {"__version__", "literal"}
    ignore = {"SecretCache"}
    actual = set(lazy.keys())
    missing = expected - actual
    extra = actual - expected - ignore
    assert not missing, f"__lazy_imports missing entries for: {sorted(missing)}"
    assert not extra, f"__lazy_imports has unexpected entries: {sorted(extra)}"
