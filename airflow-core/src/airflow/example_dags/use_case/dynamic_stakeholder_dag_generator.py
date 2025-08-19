"""
"Tailwind" - A virtual / non existing wind park energy company that powers a farm of win-mills to
produce clean energy. The company has a strong demand to ETL sensor data from the windmills as well
as need to act on data events when base data changes or contracts with customers renew. The company
values also the DEI rules and has sustainable targets for clean energy and CO2 reduction.

Event driven case: New data is dropped on a file system that triggers data processing for wind
energy. Data is loaded and Asset events are generated.
"""
from __future__ import annotations

import json
from typing import Any

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import Asset, dag, task

# Sample data in case we don't have a data source
SAMPLE_DATA: list[dict[str, Any]] = [
    {"name": "Alice", "stakeholder": "stakeholder_a", "region": "NA", "amount_usd": 100},
    {"name": "Bob", "stakeholder": "stakeholder_b", "region": "EU", "amount_usd": 200},
    {"name": "Charlie", "stakeholder": "stakeholder_a", "region": "NA", "amount_usd": 150},
    {"name": "Dana", "stakeholder": "stakeholder_c", "region": "APAC", "amount_usd": 300},
]

# Config for the stakeholder dags
CONFIG: list[dict[str, Any]] = [
    {
        "name": "stakeholder_a",
        "filter_column": "stakeholder",
        "filter_value": "stakeholder_a",
        "data_source": {"type": "local"},
    },
    {
        "name": "stakeholder_b",
        "filter_column": "stakeholder",
        "filter_value": "stakeholder_b",
        "data_source": {
            "type": "s3",
            "bucket": "example-bucket",
            "key": "stakeholder_data.csv",
        },
    },
]

DOC_MD_DAG = (
    """
    ## Stakeholder reporting dynamic dag

    Loads and processes data for stakeholder. Shows dynamic DAGs, asset tracking, and S3/local data
    integration.

    **References:**
    - [Airflow Dag Concepts](https://airflow.apache.org/docs/apache-airflow/stable/concepts/dags.html)
    - [S3Hook](https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/_api/airflow/providers/amazon/aws/hooks/s3/index.html)
    - [Dynamic Dag Generation](https://airflow.apache.org/docs/apache-airflow/stable/howto/dynamic-dag-generation.html)
    - [Asset](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/assets.html)
    - [TaskFlow API](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html)
    """
)

LOAD_DATA_DOC_MD = (
"""
Loads stakeholder data from S3 or local sample. Demonstrates use of
[S3Hook](https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/_api/airflow/providers/amazon/aws/hooks/s3/index.html)
for data ingestion.
"""
)

TRANSFORM_AND_EMIT_DOC_MD = (
"""
Filter and transform stakeholder data, emits as
[asset](https://airflow.apache.org/docs/apache-airflow/stable/data-assets.html).
"""
)

S3_CONN_ID: str = "aws_default"

for config in CONFIG:
    dynamic_dag_id: str = f"use_case_{config['name']}_report"

    def make_subdag(dag_config: dict[str, Any], dag_id: str) -> None:
        """Create a stakeholder subdag for the given config."""
        ds: dict[str, Any] = dag_config.get("data_source", {})
        tags: list[str] = [
            "Example",
            "Storyline",
            "DynamicDagGeneration",
            "Asset",
        ] + (["S3"] if ds.get("type") == "s3" else [])

        import os

        import pandas as pd

        @dag(
            dag_id=dag_id,
            schedule="@monthly",
            tags=tags,
            description=(
                f"Dynamically generated Dag to process data for stakeholder {dag_config['name']}. "
                "Demonstrates dynamic Dags, asset tracking, and S3/local data integration."
            ),
            doc_md=DOC_MD_DAG,
        )
        def stakeholder_subdag() -> None:
            @task(doc_md=LOAD_DATA_DOC_MD)
            def load_data() -> list[dict[str, Any]]:
                if ds.get("type") == "s3":
                    s3: S3Hook = S3Hook(aws_conn_id=S3_CONN_ID)
                    file_path: str = f"/tmp/{dag_config['name']}_data.csv"
                    s3.download_file(
                        bucket_name=ds["bucket"],
                        key=ds["key"],
                        local_path=file_path,
                    )
                    df = pd.read_csv(file_path)
                else:
                    df = pd.DataFrame(SAMPLE_DATA)
                records: list[dict[str, Any]] = json.loads(
                    df.fillna("").to_json(orient="records")
                )
                return records

            @task(
                doc_md=TRANSFORM_AND_EMIT_DOC_MD,
                outlets=[Asset(f"extracted_data_{dag_config['name']}")],
            )
            def transform_and_emit(records: list[dict[str, Any]]):
                df = pd.DataFrame.from_records(records)
                filtered = df[
                    df[dag_config["filter_column"]] == dag_config["filter_value"]
                ]
                output_filename = f"{dag_config['name']}_filtered.csv"
                if ds.get("type") == "s3":
                    local_path = f"/tmp/{output_filename}"
                    filtered.to_csv(local_path, index=False)
                    s3 = S3Hook(aws_conn_id=S3_CONN_ID)
                    s3.load_file(
                        filename=local_path,
                        key=output_filename,
                        bucket_name=ds["bucket"],
                        replace=True,
                    )
                    os.remove(local_path)
                    return f"s3://{ds['bucket']}/{output_filename}"
                local_path = os.path.join(os.getcwd(), output_filename)
                filtered.to_csv(local_path, index=False)
                return local_path

            records: list[dict[str, Any]] = load_data()
            transform_and_emit(records)

        stakeholder_subdag()

    make_subdag(config, dynamic_dag_id)
