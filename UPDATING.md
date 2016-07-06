# Updating Airflow

This file documents any backwards-incompatible changes in Airflow and
assists people when migrating to a new version.


## Airflow 1.8

### Changes to Behavior

#### New DAGs are paused by default

Previously, new DAGs would be scheduled immediately. To retain the old behavior, add this to airflow.cfg:

```
[core]
dags_are_paused_at_creation = False
```

### Deprecated Features
These features are marked for deprecation. They may still work (and raise a `DeprecationWarning`), but are no longer supported and will be removed entirely in Airflow 2.0

- Hooks and operators must be imported from their respective submodules

  `airflow.operators.PigOperator` is no longer supported; `from airflow.operators.pig_operator import PigOperator` is. (AIRFLOW-31, AIRFLOW-200)

- Operators no longer accept arbitrary arguments

  Previously, `Operator.__init__()` accepted any arguments (either positional `*args` or keyword `**kwargs`) without complaint. Now, invalid arguments will be rejected. (https://github.com/apache/incubator-airflow/pull/1285)

## Airflow 1.7.1.2

### Changes to Configuration

#### Email configuration change

To continue using the default smtp email backend, change the email_backend line in your config file from:

```
[email]
email_backend = airflow.utils.send_email_smtp
```
to:
```
[email]
email_backend = airflow.utils.email.send_email_smtp
```

#### S3 configuration change

To continue using S3 logging, update your config file so:

```
s3_log_folder = s3://my-airflow-log-bucket/logs
```
becomes:
```
remote_base_log_folder = s3://my-airflow-log-bucket/logs
remote_log_conn_id = <your desired s3 connection>
```
