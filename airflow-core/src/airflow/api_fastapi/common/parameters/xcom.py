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

from typing import (
    Annotated,
)

from fastapi import Depends

from airflow.api_fastapi.common.parameters.search import (
    _PrefixSearchParam,
    _SearchParam,
    prefix_search_param_factory,
    search_param_factory,
)
from airflow.models.dag import DagModel
from airflow.models.xcom import XComModel

QueryXComKeyPatternSearch = Annotated[
    _SearchParam, Depends(search_param_factory(XComModel.key, "xcom_key_pattern"))
]

QueryXComKeyPrefixPatternSearch = Annotated[
    _PrefixSearchParam, Depends(prefix_search_param_factory(XComModel.key, "xcom_key_prefix_pattern"))
]


QueryXComDagDisplayNamePatternSearch = Annotated[
    _SearchParam, Depends(search_param_factory(DagModel.dag_display_name, "dag_display_name_pattern"))
]

QueryXComDagDisplayNamePrefixPatternSearch = Annotated[
    _PrefixSearchParam,
    Depends(prefix_search_param_factory(DagModel.dag_display_name, "dag_display_name_prefix_pattern")),
]

QueryXComRunIdPatternSearch = Annotated[
    _SearchParam, Depends(search_param_factory(XComModel.run_id, "run_id_pattern"))
]

QueryXComRunIdPrefixPatternSearch = Annotated[
    _PrefixSearchParam, Depends(prefix_search_param_factory(XComModel.run_id, "run_id_prefix_pattern"))
]

QueryXComTaskIdPatternSearch = Annotated[
    _SearchParam, Depends(search_param_factory(XComModel.task_id, "task_id_pattern"))
]

QueryXComTaskIdPrefixPatternSearch = Annotated[
    _PrefixSearchParam, Depends(prefix_search_param_factory(XComModel.task_id, "task_id_prefix_pattern"))
]
