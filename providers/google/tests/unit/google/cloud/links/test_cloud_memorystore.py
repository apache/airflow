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

from airflow.providers.google.cloud.links.cloud_memorystore import (
    MemcachedInstanceDetailsLink,
    MemcachedInstanceListLink,
    RedisInstanceDetailsLink,
    RedisInstanceListLink,
)

EXPECTED_MEMCACHED_INSTANCE_DETAILS_LINK_NAME = "Memorystore Memcached Instance"
EXPECTED_MEMCACHED_INSTANCE_DETAILS_LINK_KEY = "memcached_instance"
EXPECTED_MEMCACHED_INSTANCE_DETAILS_LINK_FORMAT_STR = (
    "/memorystore/memcached/locations/{location_id}/instances/{instance_id}/details?project={project_id}"
)
EXPECTED_MEMCACHED_INSTANCE_LIST_LINK_NAME = "Memorystore Memcached List of Instances"
EXPECTED_MEMCACHED_INSTANCE_LIST_LINK_KEY = "memcached_instances"
EXPECTED_MEMCACHED_INSTANCE_LIST_LINK_FORMAT_STR = "/memorystore/memcached/instances?project={project_id}"
EXPECTED_REDIS_INSTANCE_DETAILS_LINK_NAME = "Memorystore Redis Instance"
EXPECTED_REDIS_INSTANCE_DETAILS_LINK_KEY = "redis_instance"
EXPECTED_REDIS_INSTANCE_DETAILS_LINK_FORMAT_STR = (
    "/memorystore/redis/locations/{location_id}/instances/{instance_id}/details/overview?project={project_id}"
)
EXPECTED_REDIS_INSTANCE_LIST_LINK_NAME = "Memorystore Redis List of Instances"
EXPECTED_REDIS_INSTANCE_LIST_LINK_KEY = "redis_instances"
EXPECTED_REDIS_INSTANCE_LIST_LINK_FORMAT_STR = "/memorystore/redis/instances?project={project_id}"


class TestMemcachedInstanceDetailsLink:
    def test_class_attributes(self):
        assert MemcachedInstanceDetailsLink.name == EXPECTED_MEMCACHED_INSTANCE_DETAILS_LINK_NAME
        assert MemcachedInstanceDetailsLink.key == EXPECTED_MEMCACHED_INSTANCE_DETAILS_LINK_KEY
        assert MemcachedInstanceDetailsLink.format_str == EXPECTED_MEMCACHED_INSTANCE_DETAILS_LINK_FORMAT_STR


class TestMemcachedInstanceListLink:
    def test_class_attributes(self):
        assert MemcachedInstanceListLink.name == EXPECTED_MEMCACHED_INSTANCE_LIST_LINK_NAME
        assert MemcachedInstanceListLink.key == EXPECTED_MEMCACHED_INSTANCE_LIST_LINK_KEY
        assert MemcachedInstanceListLink.format_str == EXPECTED_MEMCACHED_INSTANCE_LIST_LINK_FORMAT_STR


class TestRedisInstanceDetailsLink:
    def test_class_attributes(self):
        assert RedisInstanceDetailsLink.name == EXPECTED_REDIS_INSTANCE_DETAILS_LINK_NAME
        assert RedisInstanceDetailsLink.key == EXPECTED_REDIS_INSTANCE_DETAILS_LINK_KEY
        assert RedisInstanceDetailsLink.format_str == EXPECTED_REDIS_INSTANCE_DETAILS_LINK_FORMAT_STR


class TestRedisInstanceListLink:
    def test_class_attributes(self):
        assert RedisInstanceListLink.name == EXPECTED_REDIS_INSTANCE_LIST_LINK_NAME
        assert RedisInstanceListLink.key == EXPECTED_REDIS_INSTANCE_LIST_LINK_KEY
        assert RedisInstanceListLink.format_str == EXPECTED_REDIS_INSTANCE_LIST_LINK_FORMAT_STR
