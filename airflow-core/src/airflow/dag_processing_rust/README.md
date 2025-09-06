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

Rust Dag Processor POC - How to run with Breeze
===============================================

1. `breeze down` and ensure that there are no Dags in `/files/dags`.
2. Copy contents from `airflow/dag_processing_rust/.env.template` to `airflow/dag_processing_rust/.env` (create a new file/rename the existing one if you don't commit it).
3. Generate 10,000 DAGs within `/files/dags`, you may utilize the following Python script within `/files` (**Important** - DAGs should not utilize Taskflow syntax):

```python
import os

NUM_OF_COPIES = 9999

# Original file content
file_content = """from datetime import datetime

from airflow.models.dag import DAG
from airflow.providers.standard.operators.python import PythonOperator

with DAG(
    dag_id="example_dag_<%id%>",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:
    task_hello = PythonOperator(
        task_id="say_hello",
        python_callable=lambda: print("hey"),
    )
    task_bye = PythonOperator(
        task_id="say_bye",
        python_callable=lambda: print("Goodbye!"),
    )

    task_hello >> task_bye
"""


# Function to create copies of the file
def create_copies(num_copies, output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    for i in range(0, num_copies):
        file_name = f"dag_copy_{i}.py"
        file_path = os.path.join(output_dir, file_name)
        with open(file_path, "w") as f:
            f.write(file_content.replace("<%id%>", str(i)))


# Create 5 copies in the 'output' directory
create_copies(NUM_OF_COPIES, "./dags/")
```

4. Run `breeze start-airflow` Dags processor is already configured to utilize the Rust POC. It should take about 1.5 minutes to process all Dags (**Warning:** the processor doesn't handle well SIGTERM signals :D).
