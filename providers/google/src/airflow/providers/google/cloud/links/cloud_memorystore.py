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

from airflow.providers.google.cloud.links.base import BaseGoogleLink

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


class MemcachedInstanceListLink(BaseGoogleLink):
    """Helper class for constructing Memorystore Memcached List of Instances Link."""

    name = "Memorystore Memcached List of Instances"
    key = "memcached_instances"
    format_str = MEMCACHED_LIST_LINK


class RedisInstanceDetailsLink(BaseGoogleLink):
    """Helper class for constructing Memorystore Redis Instance Link."""

    name = "Memorystore Redis Instance"
    key = "redis_instance"
    format_str = REDIS_LINK


class RedisInstanceListLink(BaseGoogleLink):
    """Helper class for constructing Memorystore Redis List of Instances Link."""

    name = "Memorystore Redis List of Instances"
    key = "redis_instances"
    format_str = REDIS_LIST_LINK
