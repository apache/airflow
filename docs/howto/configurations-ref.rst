 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

Configuration Reference
=======================

.. _config-ref/core:

``[core]``
^^^^^^^^^^

dags_folder
***********
The folder where your airflow pipelines live, most likely a subfolder in a code repository. This path must be absolute

hostname_callable
*****************

Hostname by providing a path to a callable, which will resolve the hostname. The format is "package:function". For example, default value "socket:getfqdn" means that result from getfqdn() of "socket" package will be used as hostname. No argument should be required in the function specified. gIf using IP address as hostname is preferred, use value "airflow.utils.net:get_host_ip_address"

default_timezone
****************

Default timezone in case supplied date times are naive. Can be utc (default), system, or any IANA timezone string (e.g. Europe/Amsterdam)

executor
*********

The executor class that airflow should use. Choices include SequentialExecutor, LocalExecutor, CeleryExecutor, DaskExecutor, KubernetesExecutor


sql_alchemy_conn
****************

The SqlAlchemy connection string to the metadata database. SqlAlchemy supports many different database engine, more information their website

sql_engine_encoding
*******************

The encoding for the databases

sql_alchemy_pool_enabled
************************

If SqlAlchemy should pool database connections.

sql_alchemy_pool_size
*********************
The SqlAlchemy pool size is the maximum number of database connections in the pool. 0 indicates no limit.

sql_alchemy_max_overflow
************************
The maximum overflow size of the pool.  When the number of checked-out connections reaches the size set in pool_size, additional connections will be returned up to this limit.  When those additional connections are returned to the pool, they are disconnected and discarded.  It follows then that the total number of simultaneous connections the pool will allow is pool_size + max_overflow, and the total number of "sleeping" connections the pool will allow is pool_size.  max_overflow can be set to -1 to indicate no overflow limit; no limit will be placed on the total number of concurrent connections. Defaults to 10.

sql_alchemy_pool_recycle
************************
The SqlAlchemy pool recycle is the number of seconds a connection can be idle in the pool before it is invalidated. This config does not apply to sqlite. If the number of DB connections is ever exceeded, a lower config value will allow the system to recover faster.

sql_alchemy_pool_pre_ping
*************************
Check connection at the start of each connection pool checkout.  Typically, this is a simple statement like "SELECT 1".  More information here: https://docs.sqlalchemy.org/en/13/core/pooling.html#disconnect-handling-pessimistic
sql_alchemy_schema
******************
The schema to use for the metadata database. SqlAlchemy supports databases with the concept of multiple schemas.

sql_alchemy_connect_args
************************

Import path for connect args in SqlAlchemy. Default to an empty dict.  This is useful when you want to configure db engine args that SqlAlchemy won't parse in connection string.  See https://docs.sqlalchemy.org/en/13/core/engines.html#sqlalchemy.create_engine.params.connect_args

parallelism
***********

The amount of parallelism as a setting to the executor. This defines the max number of task instances that should run simultaneously on this airflow installation

dag_concurrency
***************

The number of task instances allowed to run concurrently by the scheduler

dags_are_paused_at_creation
***************************

Are DAGs paused by default at creation

max_active_runs_per_dag
***********************

The maximum number of active DAG runs per DAG

load_examples
*************

Whether to load the examples that ship with Airflow. It's good to get started, but you probably want to set this to False in a production environment

plugins_folder
******************

Where your Airflow plugins are stored

fernet_key
**********

Secret key to save connection passwords in the db

donot_pickle
************

Whether to disable pickling dags

dagbag_import_timeout
*********************

How long before timing out a python file import

dag_file_processor_timeout
**************************

How long before timing out a DagFileProcessor, which processes a dag file

task_runner
***********

The class to use for running task instances in a subprocess
Can be used to de-elevate a sudo user running Airflow when executing tasks

default_impersonation
*********************

If set, tasks without a ``run_as_user`` argument will be run with this user

security
********

What security module to use (for example kerberos):

secure_mode
***********

If set to False enables some unsecure features like Charts and Ad Hoc Queries.  In 2.0 will default to True.

unit_test_mode
**************

Turn unit test mode on (overwrites many configuration options with test values at runtime)

enable_xcom_pickling
********************

Whether to enable pickling for xcom (note that this is insecure and allows for RCE exploits). This will be deprecated in Airflow 2.0 (be forced to False).

killed_task_cleanup_time
************************

When a task is killed forcefully, this is the amount of time in seconds that it has to cleanup after it is sent a SIGTERM, before it is SIGKILLED

dag_run_conf_overrides_params
*****************************

Whether to override params with dag_run.conf. If you pass some key-value pairs through ``airflow dags backfill -c`` or ``airflow dags trigger -c``, the key-value pairs will override the existing ones in params.

worker_precheck
***************

Worker initialisation check to validate Metadata Database connection

dag_discovery_safe_mode
***********************

When discovering DAGs, ignore any files that don't contain the strings ``DAG`` and ``airflow``.

default_task_retries
********************

The number of retries each task is going to have by default. Can be overridden at dag or task level.

store_serialized_dags
*********************

Whether to serialises DAGs and persist them in DB.  If set to True, Webserver reads from DB instead of parsing DAG files More details: https://airflow.apache.org/docs/stable/dag-serialization.html

min_serialized_dag_update_interval
**********************************

Updating serialized DAG can not be faster than a minimum interval to reduce database write rate.

check_slas
**********

On each dagrun check against defined SLAs

.. _config-ref/logging:

[logging]
^^^^^^^^^

base_log_folder
***************
The folder where airflow should store its log files This path must be absolute

remote_logging
**************
Airflow can store logs remotely in AWS S3, Google Cloud Storage or Elastic Search. Users must supply an Airflow connection id that provides access to the storage location. If remote_logging is set to true, see UPDATING.md for additional configuration requirements.

remote_log_conn_id
******************

remote_base_log_folder
**********************

encrypt_s3_logs
***************

logging_level
*************

fab_logging_level
*****************

Logging class
*************

Specify the class that will specify the logging configuration
This class has to be on the python classpath

logging_config_class
********************

Log format
**********

Colour the logs when the controlling terminal is a TTY.

colored_console_log
*******************

colored_log_format
******************

colored_formatter_class
***********************


log_format
**********

simple_log_format
*****************


task_log_prefix_template
************************

Specify prefix pattern like mentioned below with stream handler TaskHandlerWithCustomFormatter


log_filename_template
*********************
Log filename format

log_processor_filename_template
*******************************

dag_processor_manager_log_location
**********************************

Name of handler to read task instance logs. Default to use task handler.

task_log_reader
***************

cli
***

api_client
**********

endpoint_url
************

In what way should the cli access the API. The LocalClient will use the database directly, while the json_client will use the api running on the webserver

If you set web_server_url_prefix, do NOT forget to append it here, ex: endpoint_url. So api will look like: http://localhost:8080/myroot/api/experimental/...

.. _config-ref/debug:

[debug]
^^^^^^^

fail_fast
*********
Used only with DebugExecutor. If set to True DAG will fail with first failed task. Helpful for debugging purposes.

.. _config-ref/api:

[api]
^^^^^
auth_backend
************
How to authenticate users of the API

lineage

backend
*******

what lineage backend to use

.. _config-ref/atlas:

[atlas]
^^^^^^^
sasl_enabled
************
host
****
port
****
username
********
password
********

.. _config-ref/operators:

[operators]
^^^^^^^^^^^

default_owner
*************
The default owner assigned to each new operator, unless provided explicitly or passed via ``default_args``

default_cpus
************
default_ram
***********
default_disk
************
default_gpus
************

allow_illegal_arguments
***********************
Is allowed to pass additional/unused arguments (args, kwargs) to the BaseOperator operator. If set to False, an exception will be thrown, otherwise only the console message will be displayed.

.. _config-ref/hive:

[hive]
^^^^^^

default_hive_mapred_queue
*************************
Default mapreduce queue for HiveOperator tasks
mapred_job_name_template
************************
Template for mapred_job_name in HiveOperator, supports the following named parameters: hostname, dag_id, task_id, execution_date

.. _config-ref/webserver:

[webserver]
^^^^^^^^^^^

base_url
********
The base url of your website as airflow cannot guess what domain or cname you are using. This is used in automated emails that airflow sends to point links to the right web server
web_server_host
***************
The ip specified when starting the web server

web_server_port
***************

The port on which to run the web server

web_server_ssl_cert
*******************
Paths to the SSL certificate and key for the web server. When both are provided SSL will be enabled. This does not change the web server port.

web_server_ssl_key
******************

web_server_master_timeout
*************************
Number of seconds the webserver waits before killing gunicorn master that doesn't respond

web_server_worker_timeout
*************************

Number of seconds the gunicorn webserver waits before timing out on a worker
worker_refresh_batch_size
*************************
Number of workers to refresh at a time. When set to 0, worker refresh is disabled. When nonzero, airflow periodically refreshes webserver workers by bringing up new ones and killing old ones.

worker_refresh_interval
***********************

Number of seconds to wait before refreshing a batch of workers.
secret_key
**********

Secret key used to run your flask app. It should be as random as possible
workers
*******

Number of workers to run the Gunicorn web server

worker_class
************
The worker class gunicorn should use. Choices include sync (default), eventlet, gevent
access_logfile
**************

Log files for the gunicorn webserver. '-' means log to stderr.

error_logfile
*************

expose_config
*************
Expose the configuration file in the web server

expose_hostname
***************

Expose hostname in the web server

expose_stacktrace
*****************

Expose stacktrace in the web server

dag_default_view
****************

Default DAG view.  Valid values are: tree, graph, duration, gantt, landing_times

dag_orientation
***************

Default DAG orientation. Valid values are: LR (Left->Right), TB (Top->Bottom), RL (Right->Left), BT (Bottom->Top)

demo_mode
*********
Puts the webserver in demonstration mode; blurs the names of Operators for privacy.

log_fetch_timeout_sec
*********************
The amount of time (in secs) webserver will wait for initial handshake while fetching logs from other worker machine

hide_paused_dags_by_default
***************************
By default, the webserver shows paused DAGs. Flip this to hide paused DAGs by default

page_size
*********
Consistent page size across all listing views in the UI

navbar_color
************

Define the color of navigation bar

default_dag_run_display_number
******************************
Default dagrun to show in UI

enable_proxy_fix
****************
Enable werkzeug ``ProxyFix`` middleware


cookie_secure
*************
Set secure flag on session cookie

cookie_samesite
***************
Set samesite policy on session cookie

default_wrap
************
Default setting for wrap toggle on DAG code and TI log views.

analytics_tool
**************
Send anonymous user activity to your analytics tool

analytics_id
*************

update_fab_perms
****************
Update FAB permissions and sync security manager roles on webserver startup

force_log_out_after
*******************
Minutes of non-activity before logged out from UI 0 means never get forcibly logged out


.. _config-ref/email:

[email]
^^^^^^^

email_backend
*************

.. _config-ref/smtp:

[smtp]
^^^^^^

smtp_starttls
*************
If you want airflow to send emails on retries, failure, and you want to use the airflow.utils.email.send_email_smtp function, you have to configure an

smtp_ssl
********
smtp_user
*********
Uncomment and set the user/pass settings if you want to use SMTP AUTH
smtp_password
*************
smtp_port
*********
smtp_mail_from
**************

.. _config-ref/sentry:

[sentry]
^^^^^^^^
sentry_dsn
**********
Sentry (https://docs.sentry.io) integration


.. _config-ref/celery:

[celery]
^^^^^^^^
This section only applies if you are using the CeleryExecutor in
[core] section above

celery_app_name
***************
The app name that will be used by celery

worker_concurrency
******************
The concurrency that will be used when starting workers with the "airflow celery worker" command. This defines the number of task instances that a worker will take, so size up your workers based on the resources on your worker box and the nature of your tasks

worker_autoscale
****************
The maximum and minimum concurrency that will be used when starting workers with the "airflow celery worker" command (always keep minimum processes, but grow to maximum if necessary). Note the value should be "max_concurrency,min_concurrency" Pick these numbers based on resources on worker box and the nature of the task. If autoscale option is available, worker_concurrency will be ignored. http://docs.celeryproject.org/en/latest/reference/celery.bin.worker.html#cmdoption-celery-worker-autoscale

worker_log_server_port
**********************
When you start an airflow worker, airflow starts a tiny web server subprocess to serve the workers local log files to the airflow main web server, who then builds pages and sends them to users. This defines the port on which the logs are served. It needs to be unused, and open visible from the main web server to connect into the workers.

broker_url
**********
The Celery broker URL. Celery supports RabbitMQ, Redis and experimentally a sqlalchemy database. Refer to the Celery documentation for more information. http://docs.celeryproject.org/en/latest/userguide/configuration.html#broker-settings

result_backend
**************
The Celery result_backend. When a job finishes, it needs to update the metadata of the job. Therefore it will post a message on a message bus, or insert it into a database (depending of the backend) This status is used by the scheduler to update the state of the task The use of a database is highly recommended http://docs.celeryproject.org/en/latest/userguide/configuration.html#task-result-backend-settings

flower_host
***********
Celery Flower is a sweet UI for Celery. Airflow has a shortcut to start it ``airflow flower``. This defines the IP that Celery Flower runs on

flower_url_prefix
*****************
The root URL for Flower Ex: flower_url_prefix

flower_port
***********
This defines the port that Celery Flower runs on

Securing Flower with Basic Authentication
Accepts user:password pairs separated by a comma
Example: flower_basic_auth
flower_basic_auth

default_queue
*************
Default queue that tasks get assigned to and that worker listen on.

sync_parallelism
****************
How many processes CeleryExecutor uses to sync task state. 0 means to use max(1, number of cores - 1) processes.

celery_config_options
*********************
Import path for celery configuration options

.. _config-ref/in_case_of_ssl :

[In case of using SSL]
^^^^^^^^^^^^^^^^^^^^^^
ssl_active
**********

ssl_key
*******

ssl_cacert
**********
ssl_cert
********


.. _config-ref/celery_pool_imp :

[Celery Pool implementation.]
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Choices include: prefork (default), eventlet, gevent or solo.
See:
https://docs.celeryproject.org/en/latest/userguide/workers.html#concurrency
https://docs.celeryproject.org/en/latest/userguide/concurrency/eventlet.html
pool

.. _config-ref/celery_broker_transport_options:

[celery_broker_transport_options]
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
This section is for specifying options which can be passed to the
underlying celery broker transport.  See:
http://docs.celeryproject.org/en/latest/userguide/configuration.html#std:setting-broker_transport_options

visibility_timeout
******************

The visibility timeout defines the number of seconds to wait for the worker to acknowledge the task before the message is redelivered to another worker. Make sure to increase the visibility timeout to match the time of the longest ETA you're planning to use.

visibility_timeout is only supported for Redis and SQS celery brokers.
See:
http://docs.celeryproject.org/en/master/userguide/configuration.html#std:setting-broker_transport_options


.. _config-ref/dask:

[dask]
^^^^^^

This section only applies if you are using the DaskExecutor in
[core] section above

cluster_address
***************
The IP address and port of the Dask cluster's scheduler.
tls_ca
******
TLS/ SSL settings to access a secured Dask scheduler.

tls_key
*******

tls_cert
********



.. _config-ref/scheduler:

[scheduler]
^^^^^^^^^^^

job_heartbeat_sec
*****************

Task instances listen for external kill signal (when you clear tasks
from the CLI or the UI), this defines the frequency at which they should
listen (in seconds).

scheduler_heartbeat_sec
***********************
The scheduler constantly tries to trigger new tasks (look at the scheduler section in the docs for more information). This defines how often the scheduler should run (in seconds).

num_runs
********
The number of times to try to schedule each DAG file
-1 indicates unlimited number


processor_poll_interval
***********************
The number of seconds to wait between consecutive DAG file processing

min_file_process_interval
*************************
after how much time (seconds) a new DAGs should be picked up from the filesystem

dag_dir_list_interval
*********************
How often (in seconds) to scan the DAGs directory for new files. Default to 5 minutes.

print_stats_interval
********************
How often should stats be printed to the logs

scheduler_health_check_threshold
********************************
If the last scheduler heartbeat happened more than scheduler_health_check_threshold ago (in seconds), scheduler is considered unhealthy. This is used by the health check in the "/health" endpoint

child_process_log_directory
***************************


scheduler_zombie_task_threshold
*******************************
Local task jobs periodically heartbeat to the DB. If the job has not heartbeat in this many seconds, the scheduler will mark the associated task instance as failed and will re-schedule the task.

catchup_by_default
******************
max_tis_per_query
*****************
Turn off scheduler catchup by setting this to False. Default behavior is unchanged and Command Line Backfills still work, but the scheduler will not do scheduler catchup if this is False, however it can be set on a per DAG basis in the DAG definition (catchup)

This changes the batch size of queries in the scheduling main loop. If this is too high, SQL query performance may be impacted by one or more of the following:
- reversion to full table scan
- complexity of query predicate
- excessive locking

Additionally, you may hit the maximum allowable query length for your db.

Set this to 0 for no limit (not advised)


.. _config-ref/statsd:

[statsd] (https://github.com/etsy/statsd) integration settings
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
statsd_on
*********
statsd_host
***********
statsd_port
***********
statsd_prefix
*************


max_threads
***********
If you want to avoid send all the available metrics to StatsD, you can configure an allow list of prefixes to send only the metrics that start with the elements of the list (e.g: scheduler,executor,dagrun) statsd_allow_list

The scheduler can run multiple threads in parallel to schedule dags. This defines how many threads will run.


.. _config-ref/authenticate:

[authenticate]
^^^^^^^^^^^^^^

use_job_schedule
****************
Turn off scheduler use of cron intervals by setting this to False. DAGs submitted manually in the web UI or with trigger_dag will still run.

.. _config-ref/ldap:

[ldap]
^^^^^^

uri
***
set this to ldaps://<your.ldap.server>:<port>

user_filter
***********
user_name_attr
**************
group_member_attr
*****************
superuser_filter
****************
data_profiler_filter
********************
bind_user
*********
bind_password
*************
basedn
******
cacert
******
search_scope
************


ignore_malformed_schema
***********************
This setting allows the use of LDAP servers that either return a broken schema, or do not return a schema.

.. _config-ref/kerberos:

[kerberos]
^^^^^^^^^^
ccache
******

principal
*********
gets augmented with fqdn

reinit_frequency
****************
kinit_path
**********
keytab
******



.. _config-ref/e:

[github_enterprise]
^^^^^^^^^^^^^^^^^^^

api_rev
*******


.. _config-ref/admin:

[admin]
^^^^^^^

hide_sensitive_variable_fields
******************************
UI to hide sensitive variable fields when set to True

.. _config-ref/elasticsearch:

[elasticsearch]
^^^^^^^^^^^^^^^

host
****
Elasticsearch host

end_of_log_mark
***************
frontend
********
Format of the log_id, which is used to query for a given tasks logs Used to mark the end of a log stream for a task Qualified URL for an elasticsearch frontend (like Kibana) with a template argument for log_id Code will construct log_id using the log_id template from the argument above. NOTE: The code will prefix the https:// automatically, don't include that here.

write_stdout
************
Write the task logs to the stdout of the worker, rather than the default files

json_format
***********
Instead of the default log formatter, write the log lines as JSON

json_fields
***********
Log fields to also attach to the json output, if enabled

.. _config-ref/elasticsearch_configs:

[elasticsearch_configs]
^^^^^^^^^^^^^^^^^^^^^^^
use_ssl
*******
verify_certs
************


.. _config-ref/kubernetes:

[kubernetes]
^^^^^^^^^^^^
worker_container_repository
***************************
The repository, tag and imagePullPolicy of the Kubernetes Image for the Worker to Run

worker_container_tag
********************
worker_container_image_pull_policy
**********************************


delete_worker_pods
******************
If True (default), worker pods will be deleted upon termination

worker_pods_creation_batch_size
*******************************
Number of Kubernetes Worker Pod creation calls per scheduler loop

namespace
*********
The Kubernetes namespace where airflow workers should be created. Defaults to ``default``

airflow_configmap
*****************
The name of the Kubernetes ConfigMap Containing the Airflow Configuration (this file)

dags_in_image
*************
For docker image already contains DAGs, this is set to ``True``, and the worker will search for dags in dags_folder, otherwise use git sync or dags volume claim to mount DAGs

dags_volume_subpath
*******************
For either git sync or volume mounted DAGs, the worker will look in this subpath for DAGs

dags_volume_claim
*****************
For DAGs mounted via a volume claim (mutually exclusive with git-sync and host path)

logs_volume_subpath
*******************
For volume mounted logs, the worker will look in this subpath for logs

logs_volume_claim
*****************
A shared volume claim for the logs

dags_volume_host
****************
For DAGs mounted via a hostPath volume (mutually exclusive with volume claim and git-sync) Useful in local environment, discouraged in production

logs_volume_host
****************
A hostPath volume for the logs Useful in local environment, discouraged in production

env_from_configmap_ref
**********************
A list of configMapsRefs to envFrom. If more than one configMap is specified, provide a comma separated list: configmap_a,configmap_b

env_from_secret_ref
*******************
A list of secretRefs to envFrom. If more than one secret is specified, provide a comma separated list: secret_a,secret_b

git_repo
********
Git credentials and repository for DAGs mounted via Git (mutually exclusive with volume claim)

git_branch
**********

git_subpath
***********


git_sync_rev
************
The specific rev or hash the git_sync init container will checkout This becomes GIT_SYNC_REV environment variable in the git_sync init container for worker pods

git_user
********
Use git_user and git_password for user authentication or git_ssh_key_secret_name and git_ssh_key_secret_key for SSH authentication

git_password
************

git_sync_root
*************

git_sync_dest
*************

git_dags_folder_mount_point
***************************
Mount point of the volume if git-sync is being used. i.e. {AIRFLOW_HOME}/dags

To get Git-sync SSH authentication set up follow this format

.. code-block:: yaml

  airflow-secrets.yaml:
  ---
  apiVersion: v1
  kind: Secret
  metadata:
    name: airflow-secrets
    data:
      # key needs to be gitSshKey
      gitSshKey: <base64_encoded_data>
  ---
  airflow-configmap.yaml:
  apiVersion: v1
  kind: ConfigMap
  metadata:
    name: airflow-configmap
  data:
    known_hosts: |
      github.com ssh-rsa <...>
  airflow.cfg: |
  ...

git_ssh_key_secret_name
***********************
git_ssh_known_hosts_configmap_name
**********************************
git_ssh_key_secret_name
***********************
git_ssh_known_hosts_configmap_name
**********************************


#TODO FIX CODE SAMPLE

To give the git_sync init container credentials via a secret, create a secret with two fields: GIT_SYNC_USERNAME and GIT_SYNC_PASSWORD (example below) and add `git_sync_credentials_secret

Secret Example:

.. code-block:: yaml

  apiVersion: v1
  kind: Secret
  metadata:
    name: git-credentials
  data:
    GIT_SYNC_USERNAME: <base64_encoded_git_username>
    GIT_SYNC_PASSWORD: <base64_encoded_git_password>
    git_sync_credentials_secret

For cloning DAGs from git repositories into volumes: https://github.com/kubernetes/git-sync

git_sync_container_repository
*****************************
git_sync_container_tag
**********************
git_sync_init_container_name
****************************
git_sync_run_as_user
********************


worker_service_account_name
***************************
The name of the Kubernetes service account to be associated with airflow workers, if any. Service accounts are required for workers that require access to secrets or cluster resources. See the Kubernetes RBAC documentation for more: https://kubernetes.io/docs/admin/authorization/rbac/

image_pull_secrets
******************
Any image pull secrets to be given to worker pods, If more than one secret is required, provide a comma separated list: secret_a,secret_b

in_cluster
**********
Use the service account kubernetes gives to pods to connect to kubernetes cluster. It's intended for clients that expect to be running inside a pod running on kubernetes. It will raise an exception if called from a process not running in a kubernetes environment.

cluster_context
***************
When running with in_cluster options to Kubernetes client. Leave blank these to use default behaviour like ````kubectl```` has.

config_file
***********

affinity
********
Affinity configuration as a single line formatted JSON object. See the affinity model for top-level key names (e.g. ``nodeAffinity``, etc.): https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.12/#affinity-v1-core

tolerations
***********
A list of toleration objects as a single line formatted JSON array See: https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.12/#toleration-v1-core

kube_client_request_args
************************
**kwargs parameters to pass while calling a kubernetes client core_v1_api methods from Kubernetes Executor provided as a single line formatted JSON dictionary string. List of supported params in **kwargs are similar for all core_v1_apis, hence a single config variable for all apis See: https://raw.githubusercontent.com/kubernetes-client/python/master/kubernetes/client/apis/core_v1_api.py

Worker pods security context options
See:
https://kubernetes.io/docs/tasks/configure-pod-container/security-context/

run_as_user
***********
Specifies the uid to run the first process of the worker pods containers as

fs_group
********
Specifies a gid to associate with all containers in the worker pods if using a git_ssh_key_secret_name use an fs_group that allows for the key to be read, e.g. 65533

worker_annotations
******************
Annotations configuration as a single line formatted JSON object. See the naming convention in: https://kubernetes.io/docs/concepts/overview/working-with-objects/annotations/


.. _config-ref/kubernetes_node_selectors:

[kubernetes_node_selectors]
^^^^^^^^^^^^^^^^^^^^^^^^^^^
The Key-value pairs to be given to worker pods.
The worker pods will be scheduled to the nodes of the specified key-value pairs.
Should be supplied in the format: key

.. _config-ref/kubernetes_environment_variables:

[kubernetes_environment_variables]
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The scheduler sets the following environment variables into your workers. You may define as
many environment variables as needed and the kubernetes launcher will set them in the launched workers.
Environment variables in this section are defined as follows
<environment_variable_key>

For example if you wanted to set an environment variable with value ``prod`` and key
`ENVIRONMENT` you would follow the following format:
ENVIRONMENT

Additionally you may override worker airflow settings with the AIRFLOW__<SECTION>__<KEY>
formatting as supported by airflow normally.

.. _config-ref/kubernetes_secrets:

[kubernetes_secrets]
^^^^^^^^^^^^^^^^^^^^
The scheduler mounts the following secrets into your workers as they are launched by the
scheduler. You may define as many secrets as needed and the kubernetes launcher will parse the
defined secrets and mount them as secret environment variables in the launched workers.
Secrets in this section are defined as follows
<environment_variable_mount>

For example if you wanted to mount a kubernetes secret key named ``postgres_password`` from the
kubernetes secret object ``airflow-secret`` as the environment variable ``POSTGRES_PASSWORD`` into
your workers you would follow the following format:
POSTGRES_PASSWORD

Additionally you may override worker airflow settings with the AIRFLOW__<SECTION>__<KEY>
formatting as supported by airflow normally.

kubernetes_labels
^^^^^^^^^^^^^^^^^
The Key-value pairs to be given to worker pods.
The worker pods will be given these static labels, as well as some additional dynamic labels
to identify the task.
Should be supplied in the format: key
