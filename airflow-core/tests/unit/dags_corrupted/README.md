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

This folder contains DAGs for Airflow unit testing. These files contain defects that prevent the default
Python interpreter from loading this file.

To access a DAG in this folder, use the following code inside a unit test.

```python
TEST_DAG_FOLDER = os.path.join(os.path.dirname(os.path.realpath(__file__)), "dags_corrupted")

dagbag = DagBag(dag_folder=TEST_DAG_FOLDER)
dag = dagbag.get_dag(dag_id)
```
