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
"""
# Markdown Rendering Demo

Dag documentation is rendered from `doc_md` in the Airflow UI.

## Features

- Inline code such as `task_id="extract"`
- Fenced code blocks with syntax highlighting
- Display math blocks delimited by `$$`
- Mermaid diagrams
- Markdown tables

### Code

```python
extract >> transform >> load
```

### Math

$$
y = \\beta_0 + \\beta_1 x_1 + \\beta_2 x_2 + \\epsilon
$$

### Mermaid

```mermaid
flowchart LR
    A[extract] --> B[transform]
    B --> C[load]
```

### Table

| Task | Purpose |
| --- | --- |
| `extract` | Read the source data |
| `transform` | Normalize the records |
| `load` | Publish the result |
"""

from __future__ import annotations

import pendulum

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG

with DAG(
    dag_id="example_markdown_doc",
    schedule="@once",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
    doc_md=__doc__,
) as dag:
    extract = EmptyOperator(task_id="extract")
    transform = EmptyOperator(task_id="transform")
    load = EmptyOperator(task_id="load")

    extract >> transform >> load
