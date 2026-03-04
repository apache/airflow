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
Example DAG for shareholder reporting for the Tailwind wind park.

This DAG runs on a monthly schedule to generate and send reports to stakeholders.
It demonstrates dynamic DAG generation by looping over a list of stakeholders to create tasks,
and uses a custom weight rule for retries.
For more information on Dynamic DAG Generation, see the official documentation:
https://airflow.apache.org/docs/apache-airflow/stable/howto/dynamic-dag-generation.html
"""

from __future__ import annotations

import pendulum

from airflow.example_dags.plugins.decreasing_priority_weight_strategy import DecreasingPriorityStrategy
from airflow.example_dags.tailwind.settings import TURBINE_DATA_PATH
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from airflow.utils.email import send_email

STAKEHOLDERS = ["Stakeholder A", "Stakeholder B", "Stakeholder C"]

with DAG(
    dag_id="tailwind_north_01_shareholder_reporting",
    dag_display_name="Tailwind North: Shareholder Reporting",
    description="Monthly scheduled reporting pipeline with dynamic task generation.",
    doc_md=__doc__,
    schedule="@monthly",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["Example", "Tailwind", "Custom Weight Rule", "Dynamic Generation"],
) as dag:
    import pandas

    def load_data_func(**kwargs):
        print("Loading monthly data for shareholder reporting...")
        with TURBINE_DATA_PATH.open("rb") as f:
            pandas_df = pandas.read_csv(f)
        print("Data loaded into DataFrame:")
        # Push DataFrame as JSON to XCom
        kwargs["ti"].xcom_push(key="data", value=pandas_df.to_json())

    load_data_task = PythonOperator(
        task_id="load_data",
        python_callable=load_data_func,
        dag=dag,
    )

    for stakeholder in STAKEHOLDERS:

        def generate_report_func(**kwargs):
            stakeholder_name = kwargs["stakeholder"]
            safe_stakeholder_name = stakeholder_name.replace(" ", "_").lower()
            # Pull DataFrame from XCom
            import pandas as pd

            data_json = kwargs["ti"].xcom_pull(task_ids="load_data", key="data")
            data = pd.read_json(data_json)
            print(f"Generating report for {stakeholder_name}...")
            city_map = {
                "Stakeholder A": "Seattle",
                "Stakeholder B": "Portland",
                "Stakeholder C": "Vancouver",
            }
            city_filter = city_map.get(stakeholder_name, None)
            if city_filter is not None:
                filtered_data = data[data["city"] == city_filter]
                print(f"Filtered data for {stakeholder_name} with city '{city_filter}':")
            else:
                filtered_data = data
                print(f"No specific city filter for {stakeholder_name}, using all data.")
            # Push filtered DataFrame as JSON to XCom
            kwargs["ti"].xcom_push(key=f"report_{safe_stakeholder_name}", value=filtered_data.to_json())

        safe_stakeholder_name = stakeholder.replace(" ", "_").lower()
        report_task = PythonOperator(
            task_id=f"report_{safe_stakeholder_name}",
            python_callable=generate_report_func,
            dag=dag,
            retries=3,
            weight_rule=DecreasingPriorityStrategy(),
            op_kwargs={"stakeholder": stakeholder},
        )

        def send_report_func(**kwargs):
            stakeholder_name = kwargs["stakeholder"]
            safe_stakeholder_name = stakeholder_name.replace(" ", "_").lower()
            import pandas as pd

            report_json = kwargs["ti"].xcom_pull(
                task_ids=f"report_{safe_stakeholder_name}", key=f"report_{safe_stakeholder_name}"
            )
            report = pd.read_json(report_json)
            # Ensure 'timestamp' column is datetimelike
            if not pd.api.types.is_datetime64_any_dtype(report["timestamp"]):
                report["timestamp"] = pd.to_datetime(report["timestamp"])
            daily_avg = (
                report.groupby(report["timestamp"].dt.date)[["wind_speed_mph", "power_output_kw"]]
                .mean()
                .reset_index()
            )
            print(f"Daily averages for {stakeholder_name}:")
            print(daily_avg)
            subject = f"Tailwind Shareholder Report - {stakeholder_name}"
            body = f"Hello {stakeholder_name},\n\nYour monthly report is attached below.\n\nDaily averages:\n{daily_avg.to_string(index=False)}\n\nBest regards,\nTailwind Team"
            stakeholder_emails = {
                "Stakeholder A": "stakeholder.a@example.com",
                "Stakeholder B": "stakeholder.b@example.com",
                "Stakeholder C": "stakeholder.c@example.com",
            }
            to_email = stakeholder_emails.get(stakeholder_name, "default@example.com")
            send_email(to=to_email, subject=subject, html_content=body)
            print(f"Sent daily average report to {stakeholder_name} via email: {to_email}")

        send_task = PythonOperator(
            task_id=f"send_report_{safe_stakeholder_name}",
            python_callable=send_report_func,
            op_kwargs={"stakeholder": stakeholder},
            dag=dag,
        )

        load_data_task >> report_task >> send_task
