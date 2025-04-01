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

from tests_common.test_utils.otel_utils import (
    clean_task_lines,
    extract_spans_from_output,
    get_child_list_for_non_root,
    get_id_for_a_given_name,
    get_parent_child_dict,
)


class TestUtilsUnit:
    # The method that extracts the spans from the output,
    # counts that there is no indentation on the cli, when a span starts and finishes.
    example_output = """
{
    "name": "test_dag",
    "context": {
        "trace_id": "0x01f441c9c53e793e8808c77939ddbf36",
        "span_id": "0x779a3a331684439e",
        "trace_state": "[]"
    },
    "kind": "SpanKind.INTERNAL",
    "parent_id": null,
    "start_time": "2024-11-30T14:01:21.738052Z",
    "end_time": "2024-11-30T14:01:36.541442Z",
    "status": {
        "status_code": "UNSET"
    },
    "attributes": {
        "airflow.category": "DAG runs",
        "airflow.dag_run.dag_id": "otel_test_dag_with_pause",
        "airflow.dag_run.logical_date": "2024-11-30 14:01:15+00:00",
        "airflow.dag_run.run_id": "manual__2024-11-30T14:01:15.333003+00:00",
        "airflow.dag_run.queued_at": "2024-11-30 14:01:21.738052+00:00",
        "airflow.dag_run.run_start_date": "2024-11-30 14:01:22.192655+00:00",
        "airflow.dag_run.run_end_date": "2024-11-30 14:01:36.541442+00:00",
        "airflow.dag_run.run_duration": "14.348787",
        "airflow.dag_run.state": "success",
        "airflow.dag_run.external_trigger": "True",
        "airflow.dag_run.run_type": "manual",
        "airflow.dag_run.data_interval_start": "2024-11-30 14:01:15+00:00",
        "airflow.dag_run.data_interval_end": "2024-11-30 14:01:15+00:00",
        "airflow.dag_version.version": "2",
        "airflow.dag_run.conf": "{}"
    },
    "events": [
        {
            "name": "airflow.dag_run.queued",
            "timestamp": "2024-11-30T14:01:21.738052Z",
            "attributes": {}
        },
        {
            "name": "airflow.dag_run.started",
            "timestamp": "2024-11-30T14:01:22.192655Z",
            "attributes": {}
        },
        {
            "name": "airflow.dag_run.ended",
            "timestamp": "2024-11-30T14:01:36.541442Z",
            "attributes": {}
        }
    ],
    "links": [],
    "resource": {
        "attributes": {
            "telemetry.sdk.language": "python",
            "telemetry.sdk.name": "opentelemetry",
            "telemetry.sdk.version": "1.27.0",
            "host.name": "351295342ba2",
            "service.name": "Airflow"
        },
        "schema_url": ""
    }
}
{
    "name": "task_1",
    "context": {
        "trace_id": "0x01f441c9c53e793e8808c77939ddbf36",
        "span_id": "0xba9f48dcfac5d40a",
        "trace_state": "[]"
    },
    "kind": "SpanKind.INTERNAL",
    "parent_id": "0x779a3a331684439e",
    "start_time": "2024-11-30T14:01:22.220785Z",
    "end_time": "2024-11-30T14:01:34.339423Z",
    "status": {
        "status_code": "UNSET"
    },
    "attributes": {
        "airflow.category": "scheduler",
        "airflow.task.task_id": "task_1",
        "airflow.task.dag_id": "otel_test_dag_with_pause",
        "airflow.task.state": "success",
        "airflow.task.start_date": "2024-11-30 14:01:23.468047+00:00",
        "airflow.task.end_date": "2024-11-30 14:01:34.339423+00:00",
        "airflow.task.duration": 10.871376,
        "airflow.task.executor_config": "{}",
        "airflow.task.logical_date": "2024-11-30 14:01:15+00:00",
        "airflow.task.hostname": "351295342ba2",
        "airflow.task.log_url": "http://localhost:8080/dags/otel_test_dag_with_pause/grid?dag_run_id=manual__2024-11-30T14%3A01%3A15.333003%2B00%3A00&task_id=task_1&base_date=2024-11-30T14%3A01%3A15%2B0000&tab=logs",
        "airflow.task.operator": "PythonOperator",
        "airflow.task.try_number": 1,
        "airflow.task.executor_state": "success",
        "airflow.task.pool": "default_pool",
        "airflow.task.queue": "default",
        "airflow.task.priority_weight": 2,
        "airflow.task.queued_dttm": "2024-11-30 14:01:22.216965+00:00",
        "airflow.task.queued_by_job_id": 1,
        "airflow.task.pid": 1748
    },
    "events": [
        {
            "name": "task to trigger",
            "timestamp": "2024-11-30T14:01:22.220873Z",
            "attributes": {
                "command": "['airflow', 'tasks', 'run', 'otel_test_dag_with_pause', 'task_1', 'manual__2024-11-30T14:01:15.333003+00:00', '--local', '--subdir', 'DAGS_FOLDER/otel_test_dag_with_pause.py', '--carrier', '{\"traceparent\": \"00-01f441c9c53e793e8808c77939ddbf36-ba9f48dcfac5d40a-01\"}']",
                "conf": "{}"
            }
        },
        {
            "name": "airflow.task.queued",
            "timestamp": "2024-11-30T14:01:22.216965Z",
            "attributes": {}
        },
        {
            "name": "airflow.task.started",
            "timestamp": "2024-11-30T14:01:23.468047Z",
            "attributes": {}
        },
        {
            "name": "airflow.task.ended",
            "timestamp": "2024-11-30T14:01:34.339423Z",
            "attributes": {}
        }
    ],
    "links": [
        {
            "context": {
                "trace_id": "0x01f441c9c53e793e8808c77939ddbf36",
                "span_id": "0x779a3a331684439e",
                "trace_state": "[]"
            },
            "attributes": {
                "meta.annotation_type": "link",
                "from": "parenttrace"
            }
        }
    ],
    "resource": {
        "attributes": {
            "telemetry.sdk.language": "python",
            "telemetry.sdk.name": "opentelemetry",
            "telemetry.sdk.version": "1.27.0",
            "host.name": "351295342ba2",
            "service.name": "Airflow"
        },
        "schema_url": ""
    }
}
{
    "name": "start_new_processes",
    "context": {
        "trace_id": "0x3f6d11237d2b2b8cb987e7ec923a4dc4",
        "span_id": "0x0b133494760fa56d",
        "trace_state": "[]"
    },
    "kind": "SpanKind.INTERNAL",
    "parent_id": "0xcf656e5db2b777be",
    "start_time": "2024-11-30T14:01:29.316313Z",
    "end_time": "2024-11-30T14:01:29.316397Z",
    "status": {
        "status_code": "UNSET"
    },
    "attributes": {},
    "events": [],
    "links": [],
    "resource": {
        "attributes": {
            "telemetry.sdk.language": "python",
            "telemetry.sdk.name": "opentelemetry",
            "telemetry.sdk.version": "1.27.0",
            "host.name": "351295342ba2",
            "service.name": "Airflow"
        },
        "schema_url": ""
    }
}
{
    "name": "task_2",
    "context": {
        "trace_id": "0x01f441c9c53e793e8808c77939ddbf36",
        "span_id": "0xe573c104743b6d34",
        "trace_state": "[]"
    },
    "kind": "SpanKind.INTERNAL",
    "parent_id": "0x779a3a331684439e",
    "start_time": "2024-11-30T14:01:34.698666Z",
    "end_time": "2024-11-30T14:01:36.002687Z",
    "status": {
        "status_code": "UNSET"
    },
    "attributes": {
        "airflow.category": "scheduler",
        "airflow.task.task_id": "task_2",
        "airflow.task.dag_id": "otel_test_dag_with_pause",
        "airflow.task.state": "success",
        "airflow.task.start_date": "2024-11-30 14:01:35.872318+00:00",
        "airflow.task.end_date": "2024-11-30 14:01:36.002687+00:00",
        "airflow.task.duration": 0.130369,
        "airflow.task.executor_config": "{}",
        "airflow.task.logical_date": "2024-11-30 14:01:15+00:00",
        "airflow.task.hostname": "351295342ba2",
        "airflow.task.log_url": "http://localhost:8080/dags/otel_test_dag_with_pause/grid?dag_run_id=manual__2024-11-30T14%3A01%3A15.333003%2B00%3A00&task_id=task_2&base_date=2024-11-30T14%3A01%3A15%2B0000&tab=logs",
        "airflow.task.operator": "PythonOperator",
        "airflow.task.try_number": 1,
        "airflow.task.executor_state": "success",
        "airflow.task.pool": "default_pool",
        "airflow.task.queue": "default",
        "airflow.task.priority_weight": 1,
        "airflow.task.queued_dttm": "2024-11-30 14:01:34.694842+00:00",
        "airflow.task.queued_by_job_id": 3,
        "airflow.task.pid": 1950
    },
    "events": [
        {
            "name": "task to trigger",
            "timestamp": "2024-11-30T14:01:34.698810Z",
            "attributes": {
                "command": "['airflow', 'tasks', 'run', 'otel_test_dag_with_pause', 'task_2', 'manual__2024-11-30T14:01:15.333003+00:00', '--local', '--subdir', 'DAGS_FOLDER/otel_test_dag_with_pause.py', '--carrier', '{\"traceparent\": \"00-01f441c9c53e793e8808c77939ddbf36-e573c104743b6d34-01\"}']",
                "conf": "{}"
            }
        },
        {
            "name": "airflow.task.queued",
            "timestamp": "2024-11-30T14:01:34.694842Z",
            "attributes": {}
        },
        {
            "name": "airflow.task.started",
            "timestamp": "2024-11-30T14:01:35.872318Z",
            "attributes": {}
        },
        {
            "name": "airflow.task.ended",
            "timestamp": "2024-11-30T14:01:36.002687Z",
            "attributes": {}
        }
    ],
    "links": [
        {
            "context": {
                "trace_id": "0x01f441c9c53e793e8808c77939ddbf36",
                "span_id": "0x779a3a331684439e",
                "trace_state": "[]"
            },
            "attributes": {
                "meta.annotation_type": "link",
                "from": "parenttrace"
            }
        }
    ],
    "resource": {
        "attributes": {
            "telemetry.sdk.language": "python",
            "telemetry.sdk.name": "opentelemetry",
            "telemetry.sdk.version": "1.27.0",
            "host.name": "351295342ba2",
            "service.name": "Airflow"
        },
        "schema_url": ""
    }
}
{
    "name": "task_1_sub_span",
    "context": {
        "trace_id": "0x01f441c9c53e793e8808c77939ddbf36",
        "span_id": "0x7fc9e2289c7df4b8",
        "trace_state": "[]"
    },
    "kind": "SpanKind.INTERNAL",
    "parent_id": "0xba9f48dcfac5d40a",
    "start_time": "2024-11-30T14:01:34.321996Z",
    "end_time": "2024-11-30T14:01:34.324249Z",
    "status": {
        "status_code": "UNSET"
    },
    "attributes": {
        "attr1": "val1"
    },
    "events": [],
    "links": [
        {
            "context": {
                "trace_id": "0x01f441c9c53e793e8808c77939ddbf36",
                "span_id": "0xba9f48dcfac5d40a",
                "trace_state": "[]"
            },
            "attributes": {
                "meta.annotation_type": "link",
                "from": "parenttrace"
            }
        }
    ],
    "resource": {
        "attributes": {
            "telemetry.sdk.language": "python",
            "telemetry.sdk.name": "opentelemetry",
            "telemetry.sdk.version": "1.27.0",
            "host.name": "351295342ba2",
            "service.name": "Airflow"
        },
        "schema_url": ""
    }
}
{
    "name": "emit_metrics",
    "context": {
        "trace_id": "0x3f6d11237d2b2b8cb987e7ec923a4dc4",
        "span_id": "0xa19a88e8dac9645b",
        "trace_state": "[]"
    },
    "kind": "SpanKind.INTERNAL",
    "parent_id": "0xcf656e5db2b777be",
    "start_time": "2024-11-30T14:01:29.315255Z",
    "end_time": "2024-11-30T14:01:29.315290Z",
    "status": {
        "status_code": "UNSET"
    },
    "attributes": {
        "total_parse_time": 0.9342440839973278,
        "dag_bag_size": 2,
        "import_errors": 0
    },
    "events": [],
    "links": [],
    "resource": {
        "attributes": {
            "telemetry.sdk.language": "python",
            "telemetry.sdk.name": "opentelemetry",
            "telemetry.sdk.version": "1.27.0",
            "host.name": "351295342ba2",
            "service.name": "Airflow"
        },
        "schema_url": ""
    }
}
{
    "name": "dag_parsing_loop",
    "context": {
        "trace_id": "0x3f6d11237d2b2b8cb987e7ec923a4dc4",
        "span_id": "0xcf656e5db2b777be",
        "trace_state": "[]"
    },
    "kind": "SpanKind.INTERNAL",
    "parent_id": null,
    "start_time": "2024-11-30T14:01:28.382690Z",
    "end_time": "2024-11-30T14:01:29.316499Z",
    "status": {
        "status_code": "UNSET"
    },
    "attributes": {},
    "events": [
        {
            "name": "heartbeat",
            "timestamp": "2024-11-30T14:01:29.313549Z",
            "attributes": {}
        },
        {
            "name": "_kill_timed_out_processors",
            "timestamp": "2024-11-30T14:01:29.314763Z",
            "attributes": {}
        },
        {
            "name": "prepare_file_path_queue",
            "timestamp": "2024-11-30T14:01:29.315300Z",
            "attributes": {}
        },
        {
            "name": "start_new_processes",
            "timestamp": "2024-11-30T14:01:29.315941Z",
            "attributes": {}
        },
        {
            "name": "collect_results",
            "timestamp": "2024-11-30T14:01:29.316409Z",
            "attributes": {}
        },
        {
            "name": "print_stat",
            "timestamp": "2024-11-30T14:01:29.316432Z",
            "attributes": {}
        }
    ],
    "links": [],
    "resource": {
        "attributes": {
            "telemetry.sdk.language": "python",
            "telemetry.sdk.name": "opentelemetry",
            "telemetry.sdk.version": "1.27.0",
            "host.name": "351295342ba2",
            "service.name": "Airflow"
        },
        "schema_url": ""
    }
}
    """

    example_task_output = r"""
{"timestamp":"2025-03-31T18:03:17.087597","level":"info","event":"[SimpleSpanProcessor] is being used","logger":"airflow.traces.otel_tracer"}
{"timestamp":"2025-03-31T18:03:17.087693","level":"info","event":"From task sub_span2.","logger":"airflow.otel_test_dag"}
{"timestamp":"2025-03-31T18:03:17.087763","level":"info","event":"From task sub_span3.","logger":"airflow.otel_test_dag"}
{"timestamp":"2025-03-31T18:03:17.088075","level":"info","event":"[ConsoleSpanExporter] is being used","logger":"airflow.traces.otel_tracer"}
{"timestamp":"2025-03-31T18:03:17.088105","level":"info","event":"[SimpleSpanProcessor] is being used","logger":"airflow.traces.otel_tracer"}
{"timestamp":"2025-03-31T18:03:17.088168","level":"info","event":"From task sub_span4.","logger":"airflow.otel_test_dag"}
{"timestamp":"2025-03-31T18:03:17.088257","level":"info","event":"Task_1 finished.","logger":"airflow.otel_test_dag"}
{"timestamp":"2025-03-31T18:03:17.088302","level":"info","event":"Done. Returned value was: None","logger":"airflow.task.operators.airflow.decorators.python._PythonDecoratedOperator"}
{"timestamp":"2025-03-31T18:03:17.098610Z","level":"info","event":"{","chan":"stdout","logger":"task"}
{"timestamp":"2025-03-31T18:03:17.098657Z","level":"info","event":"    \"name\": \"task1_sub_span3\",","chan":"stdout","logger":"task"}
{"timestamp":"2025-03-31T18:03:17.098728Z","level":"info","event":"    \"context\": {","chan":"stdout","logger":"task"}
{"timestamp":"2025-03-31T18:03:17.098820Z","level":"info","event":"        \"trace_id\": \"0x2ff18b33906025611803bd9bf19d9c2c\",","chan":"stdout","logger":"task"}
{"timestamp":"2025-03-31T18:03:17.098876Z","level":"info","event":"        \"span_id\": \"0x26475c0322517334\",","chan":"stdout","logger":"task"}
{"timestamp":"2025-03-31T18:03:17.098927Z","level":"info","event":"        \"trace_state\": \"[]\"","chan":"stdout","logger":"task"}
{"timestamp":"2025-03-31T18:03:17.098965Z","level":"info","event":"    },","chan":"stdout","logger":"task"}
{"timestamp":"2025-03-31T18:03:17.099038Z","level":"info","event":"    \"kind\": \"SpanKind.INTERNAL\",","chan":"stdout","logger":"task"}
{"timestamp":"2025-03-31T18:03:17.099072Z","level":"info","event":"    \"parent_id\": \"0xda642ff216bf8fb1\",","chan":"stdout","logger":"task"}
{"timestamp":"2025-03-31T18:03:17.099127Z","level":"info","event":"    \"start_time\": \"2025-03-31T18:03:17.087742Z\",","chan":"stdout","logger":"task"}
{"timestamp":"2025-03-31T18:03:17.099172Z","level":"info","event":"    \"end_time\": \"2025-03-31T18:03:17.087782Z\",","chan":"stdout","logger":"task"}
{"timestamp":"2025-03-31T18:03:17.099206Z","level":"info","event":"    \"status\": {","chan":"stdout","logger":"task"}
{"timestamp":"2025-03-31T18:03:17.099272Z","level":"info","event":"        \"status_code\": \"UNSET\"","chan":"stdout","logger":"task"}
{"timestamp":"2025-03-31T18:03:17.099317Z","level":"info","event":"    },","chan":"stdout","logger":"task"}
{"timestamp":"2025-03-31T18:03:17.099360Z","level":"info","event":"    \"attributes\": {","chan":"stdout","logger":"task"}
{"timestamp":"2025-03-31T18:03:17.099396Z","level":"info","event":"        \"attr3\": \"val3\"","chan":"stdout","logger":"task"}
{"timestamp":"2025-03-31T18:03:17.099426Z","level":"info","event":"    },","chan":"stdout","logger":"task"}
{"timestamp":"2025-03-31T18:03:17.099456Z","level":"info","event":"    \"events\": [],","chan":"stdout","logger":"task"}
{"timestamp":"2025-03-31T18:03:17.099497Z","level":"info","event":"    \"links\": [],","chan":"stdout","logger":"task"}
{"timestamp":"2025-03-31T18:03:17.099531Z","level":"info","event":"    \"resource\": {","chan":"stdout","logger":"task"}
{"timestamp":"2025-03-31T18:03:17.099586Z","level":"info","event":"        \"attributes\": {","chan":"stdout","logger":"task"}
{"timestamp":"2025-03-31T18:03:17.099630Z","level":"info","event":"            \"telemetry.sdk.language\": \"python\",","chan":"stdout","logger":"task"}
{"timestamp":"2025-03-31T18:03:17.099661Z","level":"info","event":"            \"telemetry.sdk.name\": \"opentelemetry\",","chan":"stdout","logger":"task"}
{"timestamp":"2025-03-31T18:03:17.099692Z","level":"info","event":"            \"telemetry.sdk.version\": \"1.27.0\",","chan":"stdout","logger":"task"}
{"timestamp":"2025-03-31T18:03:17.099725Z","level":"info","event":"            \"host.name\": \"2f6707197a8a\",","chan":"stdout","logger":"task"}
{"timestamp":"2025-03-31T18:03:17.099757Z","level":"info","event":"            \"service.name\": \"Airflow\"","chan":"stdout","logger":"task"}
{"timestamp":"2025-03-31T18:03:17.099787Z","level":"info","event":"        },","chan":"stdout","logger":"task"}
{"timestamp":"2025-03-31T18:03:17.099818Z","level":"info","event":"        \"schema_url\": \"\"","chan":"stdout","logger":"task"}
{"timestamp":"2025-03-31T18:03:17.099873Z","level":"info","event":"    }","chan":"stdout","logger":"task"}
{"timestamp":"2025-03-31T18:03:17.099914Z","level":"info","event":"}","chan":"stdout","logger":"task"}"""

    example_task_output_after_processing = """
[SimpleSpanProcessor] is being used
From task sub_span2.
From task sub_span3.
[ConsoleSpanExporter] is being used
[SimpleSpanProcessor] is being used
From task sub_span4.
Task_1 finished.
Done. Returned value was: None
{
    \"name\": \"task1_sub_span3\",
    \"context\": {
        \"trace_id\": \"0x2ff18b33906025611803bd9bf19d9c2c\",
        \"span_id\": \"0x26475c0322517334\",
        \"trace_state\": \"[]\"
    },
    \"kind\": \"SpanKind.INTERNAL\",
    \"parent_id\": \"0xda642ff216bf8fb1\",
    \"start_time\": \"2025-03-31T18:03:17.087742Z\",
    \"end_time\": \"2025-03-31T18:03:17.087782Z\",
    \"status\": {
        \"status_code\": \"UNSET\"
    },
    \"attributes\": {
        \"attr3\": \"val3\"
    },
    \"events\": [],
    \"links\": [],
    \"resource\": {
        \"attributes\": {
            \"telemetry.sdk.language\": \"python\",
            \"telemetry.sdk.name\": \"opentelemetry\",
            \"telemetry.sdk.version\": \"1.27.0\",
            \"host.name\": \"2f6707197a8a\",
            \"service.name\": \"Airflow\"
        },
        \"schema_url\": \"\"
    }
}"""

    # In the example output, there are two parent child relationships.
    #
    # test_dag
    #   |_ task_1
    #       |_ task_1_sub_span
    #   |_ task_2
    #
    # dag_parsing_loop
    #   |_ emit_metrics
    #   |_ start_new_processes

    def test_extract_spans_from_output(self):
        output_lines = self.example_output.splitlines()
        root_span_dict, span_dict = extract_spans_from_output(output_lines)

        assert len(root_span_dict) == 2
        assert len(span_dict) == 7

        expected_root_span_names = ["test_dag", "dag_parsing_loop"]
        actual_root_span_names = []
        for key, value in root_span_dict.items():
            assert key == value["context"]["span_id"]
            assert value["parent_id"] is None
            actual_root_span_names.append(value["name"])

        assert sorted(actual_root_span_names) == sorted(expected_root_span_names)

        expected_span_names = [
            "test_dag",
            "task_1",
            "task_1_sub_span",
            "task_2",
            "dag_parsing_loop",
            "emit_metrics",
            "start_new_processes",
        ]
        actual_span_names = []
        for key, value in span_dict.items():
            assert key == value["context"]["span_id"]
            actual_span_names.append(value["name"])

        assert sorted(actual_span_names) == sorted(expected_span_names)

    def test_get_id_for_a_given_name(self):
        output_lines = self.example_output.splitlines()
        root_span_dict, span_dict = extract_spans_from_output(output_lines)

        span_name_to_test = "test_dag"

        span_id = get_id_for_a_given_name(span_dict, span_name_to_test)

        # Get the id from the two dictionaries, and then cross-reference the name.
        span_from_root_dict = root_span_dict.get(span_id)
        span_from_dict = span_dict.get(span_id)

        assert span_from_root_dict is not None
        assert span_from_dict is not None

        assert span_name_to_test == span_from_root_dict["name"]
        assert span_name_to_test == span_from_dict["name"]

    def test_get_parent_child_dict(self):
        output_lines = self.example_output.splitlines()
        root_span_dict, span_dict = extract_spans_from_output(output_lines)

        parent_child_dict = get_parent_child_dict(root_span_dict, span_dict)

        # There are two root spans. The dictionary should also have length equal to two.
        assert len(parent_child_dict) == 2

        assert sorted(root_span_dict.keys()) == sorted(parent_child_dict.keys())

        for root_span_id, child_spans in parent_child_dict.items():
            # Both root spans have two direct child spans.
            assert len(child_spans) == 2

            root_span = root_span_dict.get(root_span_id)
            root_span_trace_id = root_span["context"]["trace_id"]

            expected_child_span_names = []
            if root_span["name"] == "test_dag":
                expected_child_span_names.extend(["task_1", "task_2"])
            elif root_span["name"] == "dag_parsing_loop":
                expected_child_span_names.extend(["emit_metrics", "start_new_processes"])

            actual_child_span_names = []

            for child_span in child_spans:
                # root_span_id should be the parent.
                assert root_span_id == child_span["parent_id"]
                # all spans should have the same trace_id.
                assert root_span_trace_id == child_span["context"]["trace_id"]
                actual_child_span_names.append(child_span["name"])

            assert sorted(actual_child_span_names) == sorted(expected_child_span_names)

    def test_get_child_list_for_non_root(self):
        output_lines = self.example_output.splitlines()
        root_span_dict, span_dict = extract_spans_from_output(output_lines)

        span_name_to_test = "task_1"
        span_id = get_id_for_a_given_name(span_dict, span_name_to_test)

        assert span_name_to_test == span_dict.get(span_id)["name"]

        # The span isn't a root span.
        assert span_id not in root_span_dict.keys()
        assert span_id in span_dict.keys()

        expected_child_span_names = ["task_1_sub_span"]
        actual_child_span_names = []

        task_1_child_spans = get_child_list_for_non_root(span_dict, "task_1")

        for span in task_1_child_spans:
            actual_child_span_names.append(span["name"])

        assert sorted(actual_child_span_names) == sorted(expected_child_span_names)

    def test_clean_task_lines(self):
        output_lines = self.example_task_output.splitlines()
        cleaned_lines = clean_task_lines(output_lines)

        assert (
            cleaned_lines == self.example_task_output_after_processing.splitlines()
        ), "Cleaned task lines do not match the expected output after processing."
