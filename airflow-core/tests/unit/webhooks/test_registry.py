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

from datetime import timedelta

import pytest

from airflow.exceptions import AirflowConfigException
from airflow.webhooks.registry import (
    WebhookRegistry,
    get_cancel_requests_per_loop,
    get_default_wait_timeout,
    get_early_delivery_ttl,
    parse_source_type_owners,
    parse_webhook_names,
)

from tests_common.test_utils.config import conf_vars


class TestParseWebhookNames:
    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            pytest.param("", frozenset(), id="empty"),
            pytest.param("github", {"github"}, id="one"),
            pytest.param(" github , jira ", {"github", "jira"}, id="spaces"),
            pytest.param("a,,b,", {"a", "b"}, id="empty-items"),
            pytest.param("deploy-bot_2", {"deploy-bot_2"}, id="hyphen-underscore-digit"),
            pytest.param("9lives", {"9lives"}, id="leading-digit"),
            pytest.param("a" * 64, {"a" * 64}, id="64-characters"),
        ],
    )
    def test_valid_names(self, value, expected):
        assert parse_webhook_names(value) == expected

    @pytest.mark.parametrize(
        ("value", "message"),
        [
            pytest.param("github,*", "cannot be *", id="any-name"),
            pytest.param(
                "analytics:dbt_cloud", "'analytics:dbt_cloud', but it is qualified with a team", id="team"
            ),
            pytest.param("_health", "'_health', but it is not a webhook name", id="leading-underscore"),
            pytest.param("-x", "'-x', but it is not a webhook name", id="leading-hyphen"),
            pytest.param("GitHub", "'GitHub', but it is not a webhook name", id="uppercase"),
            pytest.param("a/b", "'a/b', but it is not a webhook name", id="slash"),
            pytest.param("a" * 65, "but it is not a webhook name", id="65-characters"),
            pytest.param("github,jira,github", "lists 'github' more than once", id="duplicate"),
            pytest.param("github,other", "lists 'other', which is reserved", id="metrics-tag"),
        ],
    )
    def test_invalid_names_are_refused(self, value, message):
        with pytest.raises(AirflowConfigException, match=r"^\[webhooks\] names") as exc_info:
            parse_webhook_names(value)

        assert message in str(exc_info.value)


class TestParseSourceTypeOwners:
    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            pytest.param("", {}, id="empty"),
            pytest.param(
                "github=apache-airflow-providers-github",
                {"github": "apache-airflow-providers-github"},
                id="one",
            ),
            pytest.param(
                " github = Apache_Airflow.Providers-GitHub ,, dbt_cloud=acme-webhooks, ",
                {"github": "apache-airflow-providers-github", "dbt_cloud": "acme-webhooks"},
                id="normalized",
            ),
        ],
    )
    def test_valid_owners(self, value, expected):
        assert parse_source_type_owners(value) == expected

    @pytest.mark.parametrize(
        ("value", "message"),
        [
            pytest.param("github", "lists 'github'; expected source_type=package", id="no-package"),
            pytest.param("github=", "lists 'github='; expected source_type=package", id="empty-package"),
            pytest.param("=acme-webhooks", "expected source_type=package", id="empty-source-type"),
            pytest.param("github=a,github=b", "source type 'github' more than once", id="duplicate"),
            pytest.param(
                "GitHub=x", "lists 'GitHub', which is not a source type", id="uppercase-source-type"
            ),
            pytest.param("git-hub=x", "lists 'git-hub', which is not a source type", id="hyphen-source-type"),
            pytest.param(f"{'a' * 65}=x", "which is not a source type: use 1 to 64", id="long-source-type"),
            pytest.param("a=b=c", "lists 'b=c', which is not a package name", id="invalid-package"),
        ],
    )
    def test_invalid_owners_are_refused(self, value, message):
        with pytest.raises(AirflowConfigException, match=r"^\[webhooks\] source_type_owners") as exc_info:
            parse_source_type_owners(value)

        assert message in str(exc_info.value)


class TestWebhookRegistry:
    @conf_vars(
        {
            ("webhooks", "names"): "github,jira",
            ("webhooks", "source_type_owners"): "github=apache_airflow_providers_github",
            ("webhooks", "connection_cache_ttl"): "0",
            ("webhooks", "max_body_bytes"): "1",
            ("webhooks", "delivery_retention"): "1",
            ("webhooks", "rate_limit_per_minute"): "600",
            ("webhooks", "store_payload"): "summary",
            ("webhooks", "max_event_payload_bytes"): "0",
            ("webhooks", "early_delivery_ttl"): "1",
            ("webhooks", "early_delivery_max_per_webhook"): "1",
        }
    )
    def test_from_config(self):
        assert WebhookRegistry.from_config() == WebhookRegistry(
            names=frozenset({"github", "jira"}),
            source_type_owners={"github": "apache-airflow-providers-github"},
            connection_cache_ttl=0,
            max_body_bytes=1,
            delivery_retention=timedelta(days=1),
            rate_limit_per_minute=600,
            store_payload="summary",
            max_event_payload_bytes=0,
            early_delivery_ttl=timedelta(seconds=1),
            early_delivery_max_per_webhook=1,
        )

    def test_from_config_defaults_to_no_webhooks(self):
        assert WebhookRegistry.from_config() == WebhookRegistry(
            names=frozenset(),
            source_type_owners={},
            connection_cache_ttl=60,
            max_body_bytes=1048576,
            delivery_retention=timedelta(days=7),
            rate_limit_per_minute=0,
            store_payload="full",
            max_event_payload_bytes=65536,
            early_delivery_ttl=timedelta(minutes=10),
            early_delivery_max_per_webhook=1000,
        )

    @pytest.mark.parametrize(
        ("option", "value", "message"),
        [
            pytest.param("connection_cache_ttl", "-1", "connection_cache_ttl must be 0 or more", id="ttl"),
            pytest.param("max_body_bytes", "0", "max_body_bytes must be 1 or more", id="max-body-bytes"),
            pytest.param("delivery_retention", "0", "delivery_retention must be 1 or more", id="retention"),
            pytest.param(
                "delivery_retention", "366", "delivery_retention must be 365 or less", id="retention-too-long"
            ),
            pytest.param(
                "rate_limit_per_minute", "-1", "rate_limit_per_minute must be 0 or more", id="rate-limit"
            ),
            pytest.param(
                "max_event_payload_bytes",
                "-1",
                "max_event_payload_bytes must be 0 or more",
                id="max-event-payload-bytes",
            ),
            pytest.param(
                "early_delivery_ttl", "0", "early_delivery_ttl must be 1 or more", id="early-delivery-ttl"
            ),
            pytest.param(
                "early_delivery_max_per_webhook",
                "0",
                "early_delivery_max_per_webhook must be 1 or more",
                id="early-delivery-max-per-webhook",
            ),
        ],
    )
    def test_from_config_refuses_values_out_of_range(self, option, value, message):
        with conf_vars({("webhooks", option): value}):
            with pytest.raises(AirflowConfigException, match=rf"^\[webhooks\] {message}, not {value}$"):
                WebhookRegistry.from_config()

    @conf_vars({("webhooks", "store_payload"): "some"})
    def test_from_config_refuses_an_unknown_store_payload(self):
        with pytest.raises(
            AirflowConfigException,
            match=r"^\[webhooks\] store_payload must be one of full, summary, none, not 'some'$",
        ):
            WebhookRegistry.from_config()


class TestWaitOptions:
    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            pytest.param("", None, id="unset"),
            pytest.param("3600", timedelta(hours=1), id="seconds"),
            pytest.param("31536000", timedelta(days=365), id="maximum"),
        ],
    )
    def test_default_wait_timeout(self, value, expected):
        with conf_vars({("webhooks", "default_wait_timeout"): value}):
            assert get_default_wait_timeout() == expected

    def test_early_delivery_ttl_defaults_to_ten_minutes(self):
        assert get_early_delivery_ttl() == timedelta(minutes=10)

    @pytest.mark.parametrize(
        ("option", "get"),
        [
            pytest.param("default_wait_timeout", get_default_wait_timeout, id="default-wait-timeout"),
            pytest.param("early_delivery_ttl", get_early_delivery_ttl, id="early-delivery-ttl"),
        ],
    )
    def test_refuses_a_value_below_one_second(self, option, get):
        with conf_vars({("webhooks", option): "0"}):
            with pytest.raises(
                AirflowConfigException, match=rf"^\[webhooks\] {option} must be 1 or more, not 0$"
            ):
                get()

    @conf_vars({("webhooks", "default_wait_timeout"): "31536001"})
    def test_refuses_a_default_wait_timeout_above_365_days(self):
        with pytest.raises(
            AirflowConfigException,
            match=r"^\[webhooks\] default_wait_timeout must be 31536000 or less, not 31536001$",
        ):
            get_default_wait_timeout()


class TestCancelOptions:
    def test_cancel_requests_per_loop_defaults_to_20(self):
        assert get_cancel_requests_per_loop() == 20

    @conf_vars({("webhooks", "cancel_requests_per_loop"): "0"})
    def test_refuses_cancel_requests_per_loop_below_1(self):
        with pytest.raises(
            AirflowConfigException, match=r"^\[webhooks\] cancel_requests_per_loop must be 1 or more, not 0$"
        ):
            get_cancel_requests_per_loop()
