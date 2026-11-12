<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Performance-sensitive components

Some code runs for every task, every Task SDK call, every log line or every scheduler loop, so a
slowdown of a few microseconds there multiplies across every deployment. Passing tests do not show
it. Do not skip the measurement when the diff touches one of these:

| Component | Path | Runs for |
|---|---|---|
| Task SDK comms, supervisor and task runner | `task-sdk/src/airflow/sdk/execution_time/` | every Variable, Connection and XCom call of a task |
| Execution API | `airflow-core/src/airflow/api_fastapi/execution_api/` | the server side of every Task SDK call |
| Logging and secret masking | `shared/logging/`, `shared/secrets_masker/` | every log line (see [#71721](https://github.com/apache/airflow/issues/71721)) |
| Serialization | `airflow-core/src/airflow/serialization/` | every Dag parse, every Dag load in the scheduler and API server |
| Scheduler and triggerer loops | `airflow-core/src/airflow/jobs/` | every loop iteration |
| Dag processing | `airflow-core/src/airflow/dag_processing/` | every Dag file parse |

To measure:

1. Time the changed code path on the base commit and on the change with the same script. Alternate
   base and head runs, at least three of each, and compare the medians. Use `timeit` for calls
   under a microsecond, and the helpers in `devel-common/src/tests_common/test_utils/perf/perf_kit/`
   (`timing`, `repeat`, `profiled`, `trace_memory`) for longer ones.
2. In the same runs, time something the change does not touch as a control. If the control moved by
   more than about 10%, the machine was noisy: run again before drawing conclusions.
3. For code that queries the database, also pin the number of queries in the tests with
   `assert_queries_count` from `tests_common.test_utils.asserts`.
4. Put a before/after table in the PR description. If the change is slower, say by how much and
   why that is acceptable. Do not commit the timing script unless a reviewer asks for it.
