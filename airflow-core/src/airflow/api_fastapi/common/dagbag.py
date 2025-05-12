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

from typing import Annotated

from fastapi import Depends, Request

from airflow.models.dagbag import DagBag
from airflow.settings import DAGS_FOLDER


def create_dag_bag() -> DagBag:
    """Create DagBag to retrieve DAGs from the database."""
    return DagBag(DAGS_FOLDER, read_dags_from_db=True)


def dag_bag_from_app(request: Request) -> DagBag:
    """
    FastAPI dependency resolver that returns the shared DagBag instance from app.state.

    This ensures that all API routes using DagBag via dependency injection receive the same
    singleton instance that was initialized at app startup.
    """
    return request.app.state.dag_bag


DagBagDep = Annotated[DagBag, Depends(dag_bag_from_app)]
