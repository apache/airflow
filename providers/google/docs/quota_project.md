# Quota Project Support for Google Cloud Providers

Starting with Airflow {version}, Google Cloud providers now support specifying a quota project (billing project) for Google Cloud services. This feature is particularly useful for organizations that use shared service accounts and need to bill API usage to specific projects.

## Background

When using Google Cloud services, the quota and billing for API calls are typically charged to the project that owns the service account. However, in some scenarios, you might want to charge API usage to a different project. This is where quota projects come in.

## Usage

There are two ways to specify a quota project:

### 1. Via Connection Configuration

You can add the quota project ID in your Google Cloud connection's extra field:

```json
{
    "quota_project_id": "your-billing-project-id"
}
```

This can be done through the Airflow UI or via an environment variable:

```bash
export AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT='{
    "conn_type": "google-cloud-platform",
    "extra": {
        "quota_project_id": "your-billing-project-id"
    }
}'
```

### 2. Via Operator/Hook Parameters

You can specify the quota project directly when instantiating operators or hooks:

```python
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

# Using with an operator
task = BigQueryExecuteQueryOperator(
    task_id='execute_query',
    sql='SELECT * FROM `my_project.dataset.table`',
    quota_project_id='your-billing-project-id'
)

# Using with a hook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

hook = BigQueryHook(quota_project_id='your-billing-project-id')
```

## Priority

If both connection configuration and parameter are provided, the parameter value takes precedence.

## Compatibility

This feature works with all Google Cloud services that support quota project specification through the `x-goog-user-project` header, including:

- BigQuery
- Cloud Storage
- Dataflow 
- And other Google Cloud APIs

## Impact

The quota project setting affects:
- API usage billing
- Quota limits
- Monitoring and auditing

## Example DAG

Here's a complete example showing different ways to use quota projects:

```python
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from datetime import datetime

with DAG('example_quota_project', start_date=datetime(2025, 1, 1)) as dag:
    # Using quota project from connection
    query_task = BigQueryExecuteQueryOperator(
        task_id='query_with_conn_quota',
        sql='SELECT CURRENT_TIMESTAMP()',
        gcp_conn_id='google_cloud_with_quota'  # Connection has quota_project_id in extras
    )

    # Using explicit quota project
    create_bucket = GCSCreateBucketOperator(
        task_id='create_bucket',
        bucket_name='my-bucket',
        quota_project_id='explicit-billing-project'
    )

    query_task >> create_bucket
```

## Troubleshooting

If you encounter issues:

1. Verify that the quota project exists and has billing enabled
2. Ensure the service account has necessary permissions on the quota project
3. Check that the API is enabled in the quota project
4. Verify the correct quota project ID is being used by enabling debug logging

## Migration Guide

This feature is backward compatible. Existing DAGs will continue to work as before, with API calls being billed to the project that owns the service account.