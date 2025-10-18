#
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

from dataclasses import is_dataclass
from typing import Any


def is_pydantic_model(cls: Any) -> bool:
    """
    Return True if the class is a pydantic.main.BaseModel.

    Checking is done by attributes as it is significantly faster than
    using isinstance.
    """
    # __pydantic_fields__ is always present on Pydantic V2 models and is a dict[str, FieldInfo]
    # __pydantic_validator__ is an internal validator object, always set after model build
    # Check if it is not a dataclass to prevent detecting pydantic dataclasses as pydantic models
    return (
        hasattr(cls, "__pydantic_fields__")
        and hasattr(cls, "__pydantic_validator__")
        and not is_dataclass(cls)
    )
