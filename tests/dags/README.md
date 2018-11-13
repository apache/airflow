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

# Unit Tests DAGs Folder

This folder contains DAGs for Airflow unit testing.

To access a DAG in this folder, use the following code inside a unit test. Note this only works when `test_mode` is on; otherwise the normal Airflow `DAGS_FOLDER` will take precedence.

```python
dagbag = DagBag()
dag = dagbag.get_dag(dag_id)
```
