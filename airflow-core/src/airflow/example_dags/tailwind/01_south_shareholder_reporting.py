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
from airflow.sdk import DAG, task

STAKEHOLDERS = ["Stakeholder A", "Stakeholder B", "Stakeholder C"]

with DAG(
    dag_id="tailwind_south_01_shareholder_reporting",
    dag_display_name="Tailwind South: Shareholder Reporting",
    description="Monthly scheduled reporting pipeline with dynamic task generation.",
    doc_md=__doc__,
    schedule="@monthly",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["Example", "Tailwind", "Custom Weight Rule", "Dynamic Generation"],
) as dag:
    import pandas

    @task(task_display_name="Load Data")
    def load_data() -> pandas.DataFrame:
        """
        Loading the monthly data required for reporting.
        """
        from airflow.example_dags.tailwind.settings import TURBINE_DATA_PATH

        print("Loading monthly data for shareholder reporting...")
        with TURBINE_DATA_PATH.open("rb") as f:
            pandas_df = pandas.read_csv(f)
        print("Data loaded into DataFrame:")

        return pandas_df

    load_data_task = load_data()

    for stakeholder in STAKEHOLDERS:
        safe_stakeholder_name = stakeholder.replace(" ", "_").lower()

        @task(
            task_id=f"report_{safe_stakeholder_name}",
            task_display_name=f"Generate Report for {stakeholder}",
            weight_rule=DecreasingPriorityStrategy(),
            retries=3,
        )
        def generate_report(data: pandas.DataFrame, stakeholder_name: str = stakeholder) -> pandas.DataFrame:
            """
            Generating a report for a specific stakeholder.
            """
            print(f"Generating report for {stakeholder_name}...")
            # Filter data by stakeholder and city
            # Example: For Stakeholder A, filter city == 'Seattle'
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

            return filtered_data

        @task(
            task_id=f"send_report_{safe_stakeholder_name}",
            task_display_name=f"Send Report to {stakeholder}",
        )
        def send_report(report: pandas.DataFrame, stakeholder_name: str = stakeholder) -> str:
            """
            Sending the generated report to the stakeholder.
            """

            # Ensure 'timestamp' column is datetimelike
            if not pandas.api.types.is_datetime64_any_dtype(report["timestamp"]):
                report["timestamp"] = pandas.to_datetime(report["timestamp"])
            # Calculate daily averages for wind_speed_mph and power_output_kw
            daily_avg = (
                report.groupby(report["timestamp"].dt.date)[["wind_speed_mph", "power_output_kw"]]
                .mean()
                .reset_index()
            )
            print(f"Daily averages for {stakeholder_name}:")
            print(daily_avg)

            from airflow.utils.email import send_email

            subject = f"Tailwind Shareholder Report - {stakeholder_name}"
            body = f"Hello {stakeholder_name},\n\nYour monthly report is attached below.\n\nDaily averages:\n{daily_avg.to_string(index=False)}\n\nBest regards,\nTailwind Team"

            # Stakeholder email addresses could be stored in a secure location or environment variable; hardcoding here for simplicity
            stakeholder_emails = {
                "Stakeholder A": "stakeholder.a@example.com",
                "Stakeholder B": "stakeholder.b@example.com",
                "Stakeholder C": "stakeholder.c@example.com",
            }
            to_email = stakeholder_emails.get(stakeholder_name, "default@example.com")

            send_email(to=to_email, subject=subject, html_content=body)
            print(f"Sent daily average report to {stakeholder_name} via email: {to_email}")
            return "Sent"

        report_task = generate_report(data=load_data_task)
        send_task = send_report(report=report_task)

        load_data_task >> report_task
