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

from functools import wraps
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from airflow.providers.openlineage.utils.utils import translate_airflow_asset
else:
    try:
        from airflow.providers.openlineage.utils.utils import translate_airflow_asset
    except ImportError:
        from airflow.providers.openlineage.utils.utils import translate_airflow_dataset

        def rename_asset_as_dataset(function):
            @wraps(function)
            def wrapper(*args, **kwargs):
                if "asset" in kwargs:
                    kwargs["dataset"] = kwargs.pop("asset")
                return function(*args, **kwargs)

            return wrapper

        translate_airflow_asset = rename_asset_as_dataset(translate_airflow_dataset)


__all__ = ["translate_airflow_asset"]
