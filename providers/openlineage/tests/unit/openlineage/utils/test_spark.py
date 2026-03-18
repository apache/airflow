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

import datetime as dt
from unittest import mock
from unittest.mock import MagicMock, patch

import pytest
from openlineage.client.transport.composite import CompositeConfig, CompositeTransport
from openlineage.client.transport.http import HttpConfig, HttpTransport
from openlineage.client.transport.kafka import KafkaConfig, KafkaTransport

from airflow.providers.openlineage.utils.spark import (
    _get_parent_job_information_as_spark_properties,
    _get_transport_information_as_spark_properties,
    _is_parent_job_information_present_in_spark_properties,
    _is_transport_information_present_in_spark_properties,
    inject_parent_job_information_into_spark_properties,
    inject_transport_information_into_spark_properties,
)

EXAMPLE_CONTEXT = {
    "ti": MagicMock(
        dag_id="dag_id",
        task_id="task_id",
        try_number=1,
        map_index=1,
        logical_date=dt.datetime(2024, 11, 11),
        dag_run=MagicMock(logical_date=dt.datetime(2024, 11, 11), clear_number=0),
    )
}
EXAMPLE_HTTP_TRANSPORT_CONFIG = {
    "type": "http",
    "url": "https://some-custom.url",
    "endpoint": "/api/custom",
    "timeout": 123,
    "compression": "gzip",
    "custom_headers": {
        "key1": "val1",
        "key2": "val2",
    },
    "auth": {
        "type": "api_key",
        "apiKey": "secret_123",
    },
}
EXAMPLE_KAFKA_TRANSPORT_CONFIG = {
    "type": "kafka",
    "topic": "my_topic",
    "config": {
        "bootstrap.servers": "test-kafka-hm0fo:10011,another.host-uuj0l:10012",
        "acks": "all",
        "retries": "3",
    },
    "flush": True,
    "messageKey": "some",
}
EXAMPLE_PARENT_JOB_SPARK_PROPERTIES = {
    "spark.openlineage.parentJobName": "dag_id.task_id",
    "spark.openlineage.parentJobNamespace": "default",
    "spark.openlineage.parentRunId": "01931885-2800-7be7-aa8d-aaa15c337267",
    "spark.openlineage.rootParentRunId": "01931885-2800-799d-8041-88a263ffa0d8",
    "spark.openlineage.rootParentJobName": "dag_id",
    "spark.openlineage.rootParentJobNamespace": "default",
}
EXAMPLE_TRANSPORT_SPARK_PROPERTIES = {
    "spark.openlineage.transport.type": "http",
    "spark.openlineage.transport.url": "https://some-custom.url",
    "spark.openlineage.transport.endpoint": "/api/custom",
    "spark.openlineage.transport.auth.type": "api_key",
    "spark.openlineage.transport.auth.apiKey": "Bearer secret_123",
    "spark.openlineage.transport.compression": "gzip",
    "spark.openlineage.transport.headers.key1": "val1",
    "spark.openlineage.transport.headers.key2": "val2",
    "spark.openlineage.transport.timeoutInMillis": "123000",
}

EXAMPLE_COMPOSITE_TRANSPORT_SPARK_PROPERTIES = {
    "spark.openlineage.transport.type": "composite",
    "spark.openlineage.transport.continueOnFailure": "True",
    "spark.openlineage.transport.transports.http.type": "http",
    "spark.openlineage.transport.transports.http.url": "https://some-custom.url",
    "spark.openlineage.transport.transports.http.endpoint": "/api/custom",
    "spark.openlineage.transport.transports.http.auth.type": "api_key",
    "spark.openlineage.transport.transports.http.auth.apiKey": "Bearer secret_123",
    "spark.openlineage.transport.transports.http.compression": "gzip",
    "spark.openlineage.transport.transports.http.headers.key1": "val1",
    "spark.openlineage.transport.transports.http.headers.key2": "val2",
    "spark.openlineage.transport.transports.http.timeoutInMillis": "123000",
}


def test_get_parent_job_information_as_spark_properties():
    result = _get_parent_job_information_as_spark_properties(EXAMPLE_CONTEXT)
    assert result == EXAMPLE_PARENT_JOB_SPARK_PROPERTIES


@patch("airflow.providers.openlineage.utils.spark.get_openlineage_listener")
def test_get_transport_information_as_spark_properties(mock_ol_listener):
    fake_listener = mock.MagicMock()
    mock_ol_listener.return_value = fake_listener
    fake_listener.adapter.get_or_create_openlineage_client.return_value.transport = HttpTransport(
        HttpConfig.from_dict(EXAMPLE_HTTP_TRANSPORT_CONFIG)
    )
    result = _get_transport_information_as_spark_properties()
    assert result == EXAMPLE_TRANSPORT_SPARK_PROPERTIES


@patch("airflow.providers.openlineage.utils.spark.get_openlineage_listener")
def test_get_transport_information_as_spark_properties_unsupported_transport_type(mock_ol_listener):
    kafka_config = KafkaConfig(
        topic="my_topic",
        config={
            "bootstrap.servers": "test-kafka-hm0fo:10011,another.host-uuj0l:10012",
            "acks": "all",
            "retries": "3",
        },
        flush=True,
        messageKey="some",
    )
    fake_listener = mock.MagicMock()
    mock_ol_listener.return_value = fake_listener
    fake_listener.adapter.get_or_create_openlineage_client.return_value.transport = KafkaTransport(
        kafka_config
    )
    result = _get_transport_information_as_spark_properties()
    assert result == {}


@patch("airflow.providers.openlineage.utils.spark.get_openlineage_listener")
def test_get_transport_information_as_spark_properties_composite_transport_type(mock_ol_listener):
    fake_listener = mock.MagicMock()
    mock_ol_listener.return_value = fake_listener
    fake_listener.adapter.get_or_create_openlineage_client.return_value.transport = CompositeTransport(
        CompositeConfig.from_dict(
            {"transports": {"http": EXAMPLE_HTTP_TRANSPORT_CONFIG, "kafka": EXAMPLE_KAFKA_TRANSPORT_CONFIG}}
        )
    )
    result = _get_transport_information_as_spark_properties()
    assert result == EXAMPLE_COMPOSITE_TRANSPORT_SPARK_PROPERTIES


@pytest.mark.parametrize(
    ("properties", "expected"),
    [
        (
            {"spark.openlineage.parentJobNamespace": "example_namespace"},
            True,
        ),
        (
            {"spark.openlineage.parentJobName": "some_job_name"},
            True,
        ),
        (
            {"spark.openlineage.parentRunId": "some_run_id"},
            True,
        ),
        (
            {"spark.openlineage.parentWhatever": "some_value", "some.other.property": "value"},
            True,
        ),
        (
            {"some.other.property": "value"},
            False,
        ),
        (
            {
                "spark.openlineage.parentJobNamespace": "another_namespace",
                "spark.openlineage.parentJobName": "another_job_name",
                "spark.openlineage.parentRunId": "another_run_id",
                "spark.openlineage.rootParentJobNamespace": "another_namespace",
                "spark.openlineage.rootParentJobName": "another_job_name",
                "spark.openlineage.rootParentRunId": "another_run_id",
            },
            True,
        ),
        (
            {},
            False,
        ),
    ],
)
def test_is_parent_job_information_present_in_spark_properties(properties, expected):
    assert _is_parent_job_information_present_in_spark_properties(properties) is expected


@pytest.mark.parametrize(
    ("properties", "expected"),
    [
        (
            {"spark.openlineage.transport": "example_namespace"},
            True,
        ),
        (
            {"spark.openlineage.transport.type": "some_job_name"},
            True,
        ),
        (
            {"spark.openlineage.transport.urlParams.value1": "some_run_id"},
            True,
        ),
        (
            {"spark.openlineage.transportWhatever": "some_value", "some.other.property": "value"},
            True,
        ),
        (
            {"some.other.property": "value"},
            False,
        ),
        (
            {},
            False,
        ),
    ],
)
def test_is_transport_information_present_in_spark_properties(properties, expected):
    assert _is_transport_information_present_in_spark_properties(properties) is expected


@pytest.mark.parametrize(
    ("properties", "should_inject"),
    [
        (
            {"spark.openlineage.parentJobNamespace": "example_namespace"},
            False,
        ),
        (
            {"spark.openlineage.parentJobName": "some_job_name"},
            False,
        ),
        (
            {"spark.openlineage.parentRunId": "some_run_id"},
            False,
        ),
        (
            {"spark.openlineage.parentWhatever": "some_value", "some.other.property": "value"},
            False,
        ),
        (
            {"some.other.property": "value"},
            True,
        ),
        (
            {},
            True,
        ),
        (
            {
                "spark.openlineage.parentJobNamespace": "another_namespace",
                "spark.openlineage.parentJobName": "another_job_name",
                "spark.openlineage.parentRunId": "another_run_id",
            },
            False,
        ),
    ],
)
def test_inject_parent_job_information_into_spark_properties(properties, should_inject):
    result = inject_parent_job_information_into_spark_properties(properties, EXAMPLE_CONTEXT)
    expected = {**properties, **EXAMPLE_PARENT_JOB_SPARK_PROPERTIES} if should_inject else properties
    assert result == expected


@pytest.mark.parametrize(
    ("properties", "should_inject"),
    [
        (
            {"spark.openlineage.transport": "example_namespace"},
            False,
        ),
        (
            {"spark.openlineage.transport.type": "some_job_name"},
            False,
        ),
        (
            {"spark.openlineage.transport.url": "some_run_id"},
            False,
        ),
        (
            {"spark.openlineage.transportWhatever": "some_value", "some.other.property": "value"},
            False,
        ),
        (
            {"some.other.property": "value"},
            True,
        ),
        (
            {},
            True,
        ),
    ],
)
@patch("airflow.providers.openlineage.utils.spark.get_openlineage_listener")
def test_inject_transport_information_into_spark_properties(mock_ol_listener, properties, should_inject):
    fake_listener = mock.MagicMock()
    mock_ol_listener.return_value = fake_listener
    fake_listener.adapter.get_or_create_openlineage_client.return_value.transport = HttpTransport(
        HttpConfig.from_dict(EXAMPLE_HTTP_TRANSPORT_CONFIG)
    )
    result = inject_transport_information_into_spark_properties(properties, EXAMPLE_CONTEXT)
    expected = {**properties, **EXAMPLE_TRANSPORT_SPARK_PROPERTIES} if should_inject else properties
    assert result == expected


@pytest.mark.parametrize(
    ("properties", "should_inject"),
    [
        (
            {"spark.openlineage.transport": "example_namespace"},
            False,
        ),
        (
            {"spark.openlineage.transport.type": "some_job_name"},
            False,
        ),
        (
            {"spark.openlineage.transport.url": "some_run_id"},
            False,
        ),
        (
            {"spark.openlineage.transportWhatever": "some_value", "some.other.property": "value"},
            False,
        ),
        (
            {"some.other.property": "value"},
            True,
        ),
        (
            {},
            True,
        ),
    ],
)
@patch("airflow.providers.openlineage.utils.spark.get_openlineage_listener")
def test_inject_composite_transport_information_into_spark_properties(
    mock_ol_listener, properties, should_inject
):
    fake_listener = mock.MagicMock()
    mock_ol_listener.return_value = fake_listener
    fake_listener.adapter.get_or_create_openlineage_client.return_value.transport = CompositeTransport(
        CompositeConfig(
            transports={
                "http": EXAMPLE_HTTP_TRANSPORT_CONFIG,
                "console": {"type": "console"},
            },
            continue_on_failure=True,
        )
    )
    result = inject_transport_information_into_spark_properties(properties, EXAMPLE_CONTEXT)
    expected = {**properties, **EXAMPLE_COMPOSITE_TRANSPORT_SPARK_PROPERTIES} if should_inject else properties
    assert result == expected
