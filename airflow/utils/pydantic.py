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

# This is an util module that makes Pydantic use optional. While we are using Pydantic in the airflow core
# codebase, we don't want to make it a hard dependency for all the users of the core codebase, because
# it is only used in the serialization and deserialization of the models for Internal API and for nothing
# else, and since Pydantic is a very popular library, we don't want to force the users of the core codebase
# to install specific Pydantic version - especially that a lot of libraries out there still depend on
# Pydantic 1 and our internal API uses Pydantic 2+

from __future__ import annotations

from importlib import metadata

from packaging import version


def is_pydantic_2_installed() -> bool:
    try:
        return version.parse(metadata.version("pydantic")).major == 2
    except ImportError:
        return False


if is_pydantic_2_installed():
    from pydantic import BaseModel, ConfigDict, PlainSerializer, PlainValidator, ValidationInfo
else:

    class BaseModel:  # type: ignore[no-redef]  # noqa: D101
        def __init__(self, *args, **kwargs):
            pass

    class ConfigDict:  # type: ignore[no-redef]  # noqa: D101
        def __init__(self, *args, **kwargs):
            pass

    class PlainSerializer:  # type: ignore[no-redef]  # noqa: D101
        def __init__(self, *args, **kwargs):
            pass

    class PlainValidator:  # type: ignore[no-redef]  # noqa: D101
        def __init__(self, *args, **kwargs):
            pass

    class ValidationInfo:  # type: ignore[no-redef]  # noqa: D101
        def __init__(self, *args, **kwargs):
            pass
