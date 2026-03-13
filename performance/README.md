<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

# Apache Airflow Performance Testing

This package contains performance testing utilities and DAGs for Apache Airflow.

## Overview

The performance testing framework generates DAGs for performance testing purposes. The number of DAGs, tasks, and their structure can be controlled through environment variables.

## Environment Variables

- `PERF_DAGS_COUNT` - number of DAGs to generate
- `PERF_TASKS_COUNT` - number of tasks in each DAG
- `PERF_START_DATE` - if not provided current time - `PERF_START_AGO` applies
- `PERF_START_AGO` - start time relative to current time used if PERF_START_DATE is not provided. Default `1h`
- `SCHEDULE_INTERVAL_ENV` - Schedule interval. Default `@once`
- `PERF_SHAPE` - shape of DAG. See `DagShape`. Default `NO_STRUCTURE`

## Installation

```bash
pip install -e .
```

## License

Apache License 2.0
