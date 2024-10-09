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
"""This module contains Cloud Memorystore links."""

from __future__ import annotations

from typing import TYPE_CHECKING

from airflow.providers.google.cloud.links.base import BaseGoogleLink

if TYPE_CHECKING:
    from airflow.models import BaseOperator
    from airflow.utils.context import Context

BASE_LINK = "/memorystore"
MEMCACHED_LINK = (
    BASE_LINK + "/memcached/locations/{location_id}/instances/{instance_id}/details?project={project_id}"
)
MEMCACHED_LIST_LINK = BASE_LINK + "/memcached/instances?project={project_id}"
REDIS_LINK = (
    BASE_LINK + "/redis/locations/{location_id}/instances/{instance_id}/details/overview?project={project_id}"
)
REDIS_LIST_LINK = BASE_LINK + "/redis/instances?project={project_id}"


class MemcachedInstanceDetailsLink(BaseGoogleLink):
    """Helper class for constructing Memorystore Memcached Instance Link."""

    name = "Memorystore Memcached Instance"
    key = "memcached_instance"
    format_str = MEMCACHED_LINK

    @staticmethod
    def persist(
        context: Context,
        task_instance: BaseOperator,
        instance_id: str,
        location_id: str,
        project_id: str | None,
    ):
        task_instance.xcom_push(
            context,
            key=MemcachedInstanceDetailsLink.key,
            value={"instance_id": instance_id, "location_id": location_id, "project_id": project_id},
        )


class MemcachedInstanceListLink(BaseGoogleLink):
    """Helper class for constructing Memorystore Memcached List of Instances Link."""

    name = "Memorystore Memcached List of Instances"
    key = "memcached_instances"
    format_str = MEMCACHED_LIST_LINK

    @staticmethod
    def persist(
        context: Context,
        task_instance: BaseOperator,
        project_id: str | None,
    ):
        task_instance.xcom_push(
            context,
            key=MemcachedInstanceListLink.key,
            value={"project_id": project_id},
        )


class RedisInstanceDetailsLink(BaseGoogleLink):
    """Helper class for constructing Memorystore Redis Instance Link."""

    name = "Memorystore Redis Instance"
    key = "redis_instance"
    format_str = REDIS_LINK

    @staticmethod
    def persist(
        context: Context,
        task_instance: BaseOperator,
        instance_id: str,
        location_id: str,
        project_id: str | None,
    ):
        task_instance.xcom_push(
            context,
            key=RedisInstanceDetailsLink.key,
            value={"instance_id": instance_id, "location_id": location_id, "project_id": project_id},
        )


class RedisInstanceListLink(BaseGoogleLink):
    """Helper class for constructing Memorystore Redis List of Instances Link."""

    name = "Memorystore Redis List of Instances"
    key = "redis_instances"
    format_str = REDIS_LIST_LINK

    @staticmethod
    def persist(
        context: Context,
        task_instance: BaseOperator,
        project_id: str | None,
    ):
        task_instance.xcom_push(
            context,
            key=RedisInstanceListLink.key,
            value={"project_id": project_id},
        )
