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

import pytest

from airflow.utils.otel_config import (
    OtelDataType,
    _env_vars_snapshot,
    _parse_kv_str_to_dict,
    load_metrics_config,
    load_traces_config,
)

from tests_common.test_utils.config import env_vars


def get_test_env_vars(
    data_type: OtelDataType, protocol: str, service: str, url: str, exporter: str, interval_ms: str
) -> dict[str, str]:
    type_lower = data_type.value
    type_upper = data_type.value.upper()

    otel_vars = {
        "OTEL_EXPORTER_OTLP_PROTOCOL": protocol,
        "OTEL_SERVICE_NAME": service,
        "OTEL_EXPORTER_OTLP_HEADERS": "",
        "OTEL_RESOURCE_ATTRIBUTES": "",
        "OTEL_EXPORTER_OTLP_ENDPOINT": url,
        f"OTEL_EXPORTER_OTLP_{type_upper}_ENDPOINT": url,
        f"OTEL_{type_upper}_EXPORTER": exporter,
    }

    if type_lower == "metrics":
        otel_vars["OTEL_METRIC_EXPORT_INTERVAL"] = interval_ms

    return otel_vars


class TestOtelConfig:
    @pytest.mark.parametrize(
        "data_type",
        [
            pytest.param(OtelDataType.TRACES, id="traces"),
            pytest.param(OtelDataType.METRICS, id="metrics"),
        ],
    )
    def test_env_vars_snapshot(self, data_type: OtelDataType):
        type_lower = data_type.value
        type_upper = data_type.value.upper()

        exporter = "otlp"
        service = "test_service"
        interval_ms = "30000"

        protocol1 = "grpc"
        url1 = "http://localhost:4317"

        protocol2 = "http/protobuf"
        url2 = "http://localhost:4318/v1/" + type_lower

        otel_vars = get_test_env_vars(
            data_type=data_type,
            protocol=protocol1,
            service=service,
            url=url1,
            exporter=exporter,
            interval_ms=interval_ms,
        )

        with env_vars(otel_vars):
            tuple_res = _env_vars_snapshot(data_type=data_type)
            assert exporter in tuple_res
            assert service in tuple_res
            if type_lower == "metrics":
                assert interval_ms in tuple_res

            assert url1 in tuple_res
            assert protocol1 in tuple_res

        otel_vars["OTEL_EXPORTER_OTLP_PROTOCOL"] = protocol2
        otel_vars["OTEL_EXPORTER_OTLP_ENDPOINT"] = url2
        otel_vars[f"OTEL_EXPORTER_OTLP_{type_upper}_ENDPOINT"] = url2

        # The snapshot result should change.
        with env_vars(otel_vars):
            tuple_res = _env_vars_snapshot(data_type=data_type)
            assert exporter in tuple_res
            assert service in tuple_res
            if type_lower == "metrics":
                assert interval_ms in tuple_res

            assert url1 not in tuple_res
            assert protocol1 not in tuple_res

            assert url2 in tuple_res
            assert protocol2 in tuple_res

    @pytest.mark.parametrize(
        "data_type",
        [
            pytest.param(OtelDataType.TRACES, id="traces"),
            pytest.param(OtelDataType.METRICS, id="metrics"),
        ],
    )
    def test_config_validation_no_endpoint(self, data_type: OtelDataType):
        type_lower = data_type.value
        type_upper = data_type.value.upper()

        with env_vars(
            {
                "OTEL_EXPORTER_OTLP_ENDPOINT": "",
                f"OTEL_EXPORTER_OTLP_{type_upper}_ENDPOINT": "",
            }
        ):
            if type_lower == "metrics":
                with pytest.raises(OSError) as endpointExc:
                    load_metrics_config()
            else:
                with pytest.raises(OSError) as endpointExc:
                    load_traces_config()

            assert f"OTEL_EXPORTER_OTLP_{type_upper}_ENDPOINT" in str(endpointExc.value)

    @pytest.mark.parametrize(
        "data_type",
        [
            pytest.param(OtelDataType.TRACES, id="traces"),
            pytest.param(OtelDataType.METRICS, id="metrics"),
        ],
    )
    def test_successful_config_validation(self, data_type: OtelDataType):
        type_lower = data_type.value

        url = f"http://localhost:4318/v1/{type_lower}"

        otel_vars = get_test_env_vars(
            data_type=data_type,
            protocol="grpc",
            service="Airflow",
            url=url,
            exporter="otlp",
            interval_ms="60000",
        )

        with env_vars(otel_vars):
            if type_lower == "metrics":
                config = load_metrics_config()

                # Check that the value is an int and not str.
                assert config.interval_ms != "60000"
                assert config.interval_ms == 60000
            else:
                config = load_traces_config()

            assert config.endpoint == url
            # Default values.
            assert config.service_name == "Airflow"
            assert config.protocol == "grpc"
            assert not config.headers_kv_str
            assert not config.resource_attributes_kv_str

    @pytest.mark.parametrize(
        "data_type",
        [
            pytest.param(OtelDataType.TRACES, id="traces"),
            pytest.param(OtelDataType.METRICS, id="metrics"),
        ],
    )
    def test_config_invalid_protocol(self, data_type: OtelDataType):
        type_lower = data_type.value

        url = f"http://localhost:4318/v1/{type_lower}"

        otel_vars = get_test_env_vars(
            data_type=data_type,
            protocol="json",
            service="Airflow",
            url=url,
            exporter="otlp",
            interval_ms="60000",
        )

        with env_vars(otel_vars):
            if type_lower == "metrics":
                with pytest.raises(ValueError) as protocolExc:
                    load_metrics_config()
            else:
                with pytest.raises(ValueError) as protocolExc:
                    load_traces_config()

            assert "Invalid value for OTEL_EXPORTER_OTLP_PROTOCOL" in str(protocolExc.value)

    @pytest.mark.parametrize(
        "data_type",
        [
            pytest.param(OtelDataType.TRACES, id="traces"),
            pytest.param(OtelDataType.METRICS, id="metrics"),
        ],
    )
    def test_config_invalid_endpoint_protocol_combination(self, data_type: OtelDataType, caplog):
        type_lower = data_type.value
        type_upper = data_type.value.upper()

        url = "http://localhost:4317"

        otel_vars = get_test_env_vars(
            data_type=data_type,
            protocol="http/protobuf",
            service="Airflow",
            url=url,
            exporter="otlp",
            interval_ms="60000",
        )

        with env_vars(otel_vars):
            if type_lower == "metrics":
                load_metrics_config()
            else:
                load_traces_config()

            expected_error = (
                "Invalid value for config 'OTEL_EXPORTER_OTLP_ENDPOINT' or "
                f"'OTEL_EXPORTER_OTLP_{type_upper}_ENDPOINT' with protocol "
                "value 'http/protobuf': http://localhost:4317"
            )

            assert expected_error in caplog.text

    def test_parsing_kv_str_configs(self):
        config_str = "service.name=my-service,service.version=1.0.0"

        config_dict = _parse_kv_str_to_dict(config_str)

        assert len(config_dict) == 2
        assert ("service.name", "my-service") in config_dict.items()
        assert ("service.version", "1.0.0") in config_dict.items()
