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

"""Tests for Cloud Memorystore links."""

from __future__ import annotations

from unittest import mock

from airflow.providers.google.cloud.links.base import BASE_LINK
from airflow.providers.google.cloud.links.cloud_memorystore import (
    MEMCACHED_LINK,
    MEMCACHED_LIST_LINK,
    REDIS_LINK,
    REDIS_LIST_LINK,
    MemcachedInstanceDetailsLink,
    MemcachedInstanceListLink,
    RedisInstanceDetailsLink,
    RedisInstanceListLink,
)

TEST_INSTANCE_ID = "test-instance-id"
TEST_LOCATION_ID = "test-location-id"
TEST_PROJECT_ID = "test-project-id"


class TestMemcachedInstanceDetailsLink:
    def test_class_attributes(self):
        assert MemcachedInstanceDetailsLink.key == "memcached_instance"
        assert MemcachedInstanceDetailsLink.name == "Memorystore Memcached Instance"
        assert MemcachedInstanceDetailsLink.format_str == MEMCACHED_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        MemcachedInstanceDetailsLink.persist(
            context=mock_context,
            instance_id=TEST_INSTANCE_ID,
            location_id=TEST_LOCATION_ID,
            project_id=TEST_PROJECT_ID,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="memcached_instance",
            value={
                "instance_id": TEST_INSTANCE_ID,
                "location_id": TEST_LOCATION_ID,
                "project_id": TEST_PROJECT_ID,
            },
        )

    def test_format_link(self):
        link = MemcachedInstanceDetailsLink()

        result = link._format_link(
            instance_id=TEST_INSTANCE_ID, location_id=TEST_LOCATION_ID, project_id=TEST_PROJECT_ID
        )

        assert result == BASE_LINK + MEMCACHED_LINK.format(
            instance_id=TEST_INSTANCE_ID, location_id=TEST_LOCATION_ID, project_id=TEST_PROJECT_ID
        )


class TestMemcachedInstanceListLink:
    def test_class_attributes(self):
        assert MemcachedInstanceListLink.key == "memcached_instances"
        assert MemcachedInstanceListLink.name == "Memorystore Memcached List of Instances"
        assert MemcachedInstanceListLink.format_str == MEMCACHED_LIST_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        MemcachedInstanceListLink.persist(
            context=mock_context,
            project_id=TEST_PROJECT_ID,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="memcached_instances",
            value={"project_id": TEST_PROJECT_ID},
        )

    def test_format_link(self):
        link = MemcachedInstanceListLink()

        result = link._format_link(project_id=TEST_PROJECT_ID)

        assert result == BASE_LINK + MEMCACHED_LIST_LINK.format(project_id=TEST_PROJECT_ID)


class TestRedisInstanceDetailsLink:
    def test_class_attributes(self):
        assert RedisInstanceDetailsLink.key == "redis_instance"
        assert RedisInstanceDetailsLink.name == "Memorystore Redis Instance"
        assert RedisInstanceDetailsLink.format_str == REDIS_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        RedisInstanceDetailsLink.persist(
            context=mock_context,
            instance_id=TEST_INSTANCE_ID,
            location_id=TEST_LOCATION_ID,
            project_id=TEST_PROJECT_ID,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="redis_instance",
            value={
                "instance_id": TEST_INSTANCE_ID,
                "location_id": TEST_LOCATION_ID,
                "project_id": TEST_PROJECT_ID,
            },
        )

    def test_format_link(self):
        link = RedisInstanceDetailsLink()

        result = link._format_link(
            instance_id=TEST_INSTANCE_ID, location_id=TEST_LOCATION_ID, project_id=TEST_PROJECT_ID
        )

        assert result == BASE_LINK + REDIS_LINK.format(
            instance_id=TEST_INSTANCE_ID, location_id=TEST_LOCATION_ID, project_id=TEST_PROJECT_ID
        )


class TestRedisInstanceListLink:
    def test_class_attributes(self):
        assert RedisInstanceListLink.key == "redis_instances"
        assert RedisInstanceListLink.name == "Memorystore Redis List of Instances"
        assert RedisInstanceListLink.format_str == REDIS_LIST_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        RedisInstanceListLink.persist(
            context=mock_context,
            project_id=TEST_PROJECT_ID,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="redis_instances",
            value={"project_id": TEST_PROJECT_ID},
        )

    def test_format_link(self):
        link = RedisInstanceListLink()

        result = link._format_link(project_id=TEST_PROJECT_ID)

        assert result == BASE_LINK + REDIS_LIST_LINK.format(project_id=TEST_PROJECT_ID)
