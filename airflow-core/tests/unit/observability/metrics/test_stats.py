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

import importlib
import re

import pytest

import airflow
import airflow.observability.stats
from airflow.observability.metrics import stats_utils

from tests_common.test_utils.config import conf_vars


class InvalidCustomStatsd:
    """
    This custom Statsd class is invalid because it does not subclass
    statsd.StatsClient.
    """

    def __init__(self, host=None, port=None, prefix=None):
        pass


class TestStats:
    def test_load_invalid_custom_stats_client(self):
        with conf_vars(
            {
                ("metrics", "statsd_on"): "True",
                ("metrics", "statsd_custom_client_path"): f"{__name__}.InvalidCustomStatsd",
            }
        ):
            importlib.reload(airflow._shared.observability.metrics.stats)
            factory = stats_utils.get_stats_factory(airflow.observability.stats.Stats)
            airflow.observability.stats.Stats.initialize(factory=factory)
            error_message = re.escape(
                "Your custom StatsD client must extend the statsd."
                "StatsClient in order to ensure backwards compatibility."
            )
            # we assert for Exception here instead of AirflowConfigException to not import from shared configuration
            with pytest.raises(Exception, match=error_message):
                airflow.observability.stats.Stats.incr("empty_key")
        importlib.reload(airflow._shared.observability.metrics.stats)


class TestDogStats:
    def setup_method(self):
        pytest.importorskip("datadog")

    def test_enabled_by_config(self):
        """Test that enabling this sets the right instance properties"""
        from datadog import DogStatsd

        with conf_vars(
            {
                ("metrics", "statsd_datadog_enabled"): "True",
            }
        ):
            importlib.reload(airflow.observability.stats)
            factory = stats_utils.get_stats_factory(airflow.observability.stats.Stats)
            airflow.observability.stats.Stats.initialize(factory=factory)
            assert isinstance(airflow.observability.stats.Stats.dogstatsd, DogStatsd)
            assert not hasattr(airflow.observability.stats.Stats, "statsd")
        # Avoid side-effects
        importlib.reload(airflow.observability.stats)

    def test_does_not_send_stats_using_statsd_when_statsd_and_dogstatsd_both_on(self):
        from datadog import DogStatsd

        with conf_vars(
            {
                ("metrics", "statsd_on"): "True",
                ("metrics", "statsd_datadog_enabled"): "True",
            }
        ):
            importlib.reload(airflow.observability.stats)
            factory = stats_utils.get_stats_factory(airflow.observability.stats.Stats)
            airflow.observability.stats.Stats.initialize(factory=factory)
            assert isinstance(airflow.observability.stats.Stats.dogstatsd, DogStatsd)
            assert not hasattr(airflow.observability.stats.Stats, "statsd")
        importlib.reload(airflow.observability.stats)
