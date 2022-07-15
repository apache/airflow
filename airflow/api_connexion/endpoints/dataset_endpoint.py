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

from sqlalchemy import func
from sqlalchemy.orm import Session

from airflow import Dataset
from airflow.api_connexion import security
from airflow.api_connexion.exceptions import NotFound
from airflow.api_connexion.parameters import apply_sorting, check_limit, format_parameters
from airflow.api_connexion.schemas.dataset_schema import (
    DatasetCollection,
    dataset_collection_schema,
    dataset_schema,
)
from airflow.api_connexion.types import APIResponse
from airflow.security import permissions
from airflow.utils.session import NEW_SESSION, provide_session


@security.requires_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_DATASET)])
@provide_session
def get_dataset(id: int, session: Session = NEW_SESSION) -> APIResponse:
    """Get a Dataset"""
    dataset = session.query(Dataset).get(id)
    if not dataset:
        raise NotFound(
            "Dataset not found",
            detail=f"The Dataset with id: `{id}` was not found",
        )
    return dataset_schema.dump(dataset)


@security.requires_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_DATASET)])
@format_parameters({'limit': check_limit})
@provide_session
def get_datasets(
    *, limit: int, offset: int = 0, order_by: str = "id", session: Session = NEW_SESSION
) -> APIResponse:
    """Get datasets"""
    allowed_attrs = ['id', 'uri', 'created_at', 'updated_at']

    total_entries = session.query(func.count(Dataset.id)).scalar()
    query = session.query(Dataset)
    query = apply_sorting(query, order_by, {}, allowed_attrs)
    datasets = query.offset(offset).limit(limit).all()
    return dataset_collection_schema.dump(DatasetCollection(datasets=datasets, total_entries=total_entries))
