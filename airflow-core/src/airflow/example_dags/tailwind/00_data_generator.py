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
"""
Example DAG – Tailwind: Data Generator.

Generates realistic wind-turbine sensor data using the ``faker`` library inside an isolated
:class:`~airflow.providers.standard.operators.python.PythonVirtualenvOperator`, then uploads
the resulting CSV to S3 (connection ``aws_default``) and emits an
:class:`~airflow.sdk.Asset` event so downstream pipelines can react automatically.

**Features demonstrated**

* :class:`~airflow.providers.standard.operators.python.PythonVirtualenvOperator` (``@task.virtualenv``)
  with extra ``requirements`` – keeps heavy dependencies out of the main Airflow environment.
* Asset outlets – the upload task declares ``outlets=[turbine_data_asset]``, which triggers
  :doc:`asset-scheduled DAGs </authoring-and-scheduling/asset-scheduling>`.
* Graceful fallback – if no S3 connection is configured the CSV is written to
  ``/tmp/tailwind/turbine_data.csv`` so the example runs out-of-the-box.

**Prerequisites (optional)**

Configure an ``aws_default`` Airflow connection pointing to an accessible S3 bucket named
``tailwind`` to enable the S3 upload path.
"""

from __future__ import annotations

import pendulum

# ---------------------------------------------------------------------------
# Asset definition – shared with downstream DAGs in the Tailwind story-line
# ---------------------------------------------------------------------------
from airflow.example_dags.tailwind.settings import BASE_PATH, TURBINE_DATA_FILE
from airflow.sdk import DAG, Asset, task

turbine_data_asset = Asset(f"{BASE_PATH}{TURBINE_DATA_FILE}")

with DAG(
    dag_id="tailwind_00_data_generator",
    dag_display_name="Tailwind: Data Generator",
    description="Generates fake turbine sensor data, uploads to S3 and emits an Asset event.",
    doc_md=__doc__,
    schedule="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["Example", "Tailwind", "Assets", "PythonVirtualenvOperator"],
) as dag:

    @task.virtualenv(
        task_display_name="Generate Sensor Data (virtualenv + faker)",
        requirements=["faker>=24.0.0"],
        system_site_packages=False,
    )
    def generate_sensor_data() -> str:
        """
        Generate 100 rows of realistic wind-turbine sensor data using Faker.

        Runs in an isolated virtual environment that has ``faker`` installed.
        Returns the CSV content as a plain string so it can be passed to the
        next task via XCom.

        :return: CSV string with header + 100 data rows.
        """
        import csv
        import io

        from faker import Faker

        fake = Faker()

        cities = ["Seattle", "Portland", "Vancouver"]
        statuses = ["Active", "Active", "Active", "Maintenance", "Alarm"]

        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(["timestamp", "turbine_id", "city", "wind_speed_mph", "power_output_kw", "status"])

        for i in range(1, 101):
            turbine_id = f"T-{i:03d}"
            city = fake.random_element(elements=cities)
            timestamp = fake.date_time_between(start_date="-1d", end_date="now").isoformat()
            wind_speed = round(fake.pyfloat(min_value=0.0, max_value=50.0), 2)
            power_output = round(wind_speed * fake.pyfloat(min_value=10.0, max_value=20.0), 2)
            status = fake.random_element(elements=statuses)
            writer.writerow([timestamp, turbine_id, city, wind_speed, power_output, status])

        return buf.getvalue()

    @task(
        task_display_name="Upload to Object Storage / Local Fallback",
        outlets=[turbine_data_asset],
    )
    def upload_data(csv_content: str) -> str:
        """
        Upload the CSV content using ``ObjectStoragePath`` (``aws_default`` connection).

        Falls back to a local file when the connection is not configured so the
        example runs out-of-the-box without any AWS setup.

        :param csv_content: CSV text produced by :func:`generate_sensor_data`.
        :return: The destination URI (``s3://…`` or ``file://…``).
        """
        import os

        from airflow.example_dags.tailwind.settings import TURBINE_DATA_PATH

        try:
            TURBINE_DATA_PATH.write_text(csv_content)
            print(f"Uploaded turbine data to {TURBINE_DATA_PATH}")
            return str(TURBINE_DATA_PATH)

        except Exception as exc:
            # Graceful fallback: write locally when S3 is not configured.
            print(f"Object storage upload not available ({exc}). Writing to local fallback.")
            local_dir = "/tmp/tailwind"
            os.makedirs(local_dir, exist_ok=True)
            local_path = os.path.join(local_dir, TURBINE_DATA_FILE)
            with open(local_path, "w") as fh:
                fh.write(csv_content)
            print(f"Turbine data written to {local_path}")
            return f"file://{local_path}"

    csv_data = generate_sensor_data()
    upload_data(csv_data)
