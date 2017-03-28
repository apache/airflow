# Updating Airflow

This file documents any backwards-incompatible changes in Airflow and
assists people when migrating to a new version.

## Master

### New Features

#### Dask Executor

A new DaskExecutor allows Airflow tasks to be run in Dask Distributed clusters.

### Deprecated Features
These features are marked for deprecation. They may still work (and raise a `DeprecationWarning`), but are no longer
supported and will be removed entirely in Airflow 2.0

- `post_execute()` hooks now take two arguments, `context` and `result`
  (AIRFLOW-886)

  Previously, post_execute() only took one argument, `context`.

## Airflow 1.8

### Database
The database schema needs to be upgraded. Make sure to shutdown Airflow and make a backup of your database. To
upgrade the schema issue `airflow upgradedb`.

### Upgrade systemd unit files
Systemd unit files have been updated. If you use systemd please make sure to update these.

> Please note that the webserver does not detach properly, this will be fixed in a future version.

### Tasks not starting although dependencies are met due to stricter pool checking
Airflow 1.7.1 has issues with being able to over subscribe to a pool, ie. more slots could be used than were
available. This is fixed in Airflow 1.8.0, but due to past issue jobs may fail to start although their
dependencies are met after an upgrade. To workaround either temporarily increase the amount of slots above
the the amount of queued tasks or use a new pool.

### Less forgiving scheduler on dynamic start_date
Using a dynamic start_date (e.g. `start_date = datetime.now()`) is not considered a best practice. The 1.8.0 scheduler
is less forgiving in this area. If you encounter DAGs not being scheduled you can try using a fixed start_date and
renaming your dag. The last step is required to make sure you start with a clean slate, otherwise the old schedule can
interfere.

### New and updated scheduler options
Please read through these options, defaults have changed since 1.7.1.

#### child_process_log_directory
In order the increase the robustness of the scheduler, DAGS our now processed in their own process. Therefore each 
DAG has its own log file for the scheduler. These are placed in `child_process_log_directory` which defaults to 
`<AIRFLOW_HOME>/scheduler/latest`. You will need to make sure these log files are removed.

> DAG logs or processor logs ignore and command line settings for log file locations.

#### run_duration
Previously the command line option `num_runs` was used to let the scheduler terminate after a certain amount of
loops. This is now time bound and defaults to `-1`, which means run continuously. See also num_runs.

#### num_runs
Previously `num_runs` was used to let the scheduler terminate after a certain amount of loops. Now num_runs specifies 
the number of times to try to schedule each DAG file within `run_duration` time. Defaults to `-1`, which means try
indefinitely. This is only available on the command line.

#### min_file_process_interval
After how much time should an updated DAG be picked up from the filesystem.

#### dag_dir_list_interval
How often the scheduler should relist the contents of the DAG directory. If you experience that while developing your
dags are not being picked up, have a look at this number and decrease it when necessary.

#### catchup_by_default
By default the scheduler will fill any missing interval DAG Runs between the last execution date and the current date.
This setting changes that behavior to only execute the latest interval. This can also be specified per DAG as 
`catchup = False / True`. Command line backfills will still work.

### Faulty Dags do not show an error in the Web UI

Due to changes in the way Airflow processes DAGs the Web UI does not show an error when processing a faulty DAG. To
find processing errors go the `child_process_log_directory` which defaults to `<AIRFLOW_HOME>/scheduler/latest`.

### New DAGs are paused by default

Previously, new DAGs would be scheduled immediately. To retain the old behavior, add this to airflow.cfg:

```
[core]
dags_are_paused_at_creation = False
```

### Airflow Context variable are passed to Hive config if conf is specified

If you specify a hive conf to the run_cli command of the HiveHook, Airflow add some
convenience variables to the config. In case your run a sceure Hadoop setup it might be
required to whitelist these variables by adding the following to your configuration:

```
<property> 
     <name>hive.security.authorization.sqlstd.confwhitelist.append</name>
     <value>airflow\.ctx\..*</value>
</property>
```
### Google Cloud Operator and Hook alignment

All Google Cloud Operators and Hooks are aligned and use the same client library. Now you have a single connection 
type for all kinds of Google Cloud Operators.

If you experience problems connecting with your operator make sure you set the connection type "Google Cloud Platform".

Also the old P12 key file type is not supported anymore and only the new JSON key files are supported as a service 
account.

### Deprecated Features
These features are marked for deprecation. They may still work (and raise a `DeprecationWarning`), but are no longer 
supported and will be removed entirely in Airflow 2.0

- Hooks and operators must be imported from their respective submodules

  `airflow.operators.PigOperator` is no longer supported; `from airflow.operators.pig_operator import PigOperator` is. 
  (AIRFLOW-31, AIRFLOW-200)

- Operators no longer accept arbitrary arguments

  Previously, `Operator.__init__()` accepted any arguments (either positional `*args` or keyword `**kwargs`) without 
  complaint. Now, invalid arguments will be rejected. (https://github.com/apache/incubator-airflow/pull/1285)

### Known Issues
There is a report that the default of "-1" for num_runs creates an issue where errors are reported while parsing tasks.
It was not confirmed, but a workaround was found by changing the default back to `None`.

To do this edit `cli.py`, find the following:

```
        'num_runs': Arg(
            ("-n", "--num_runs"),
            default=-1, type=int,
            help="Set the number of runs to execute before exiting"),
```

and change `default=-1` to `default=None`. Please report on the mailing list if you have this issue.

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
