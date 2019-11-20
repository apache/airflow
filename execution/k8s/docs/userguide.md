# User Guide

TODO

# FAQs

1. How do we refresh DAGs ?
Canonical way airflow supports refreshing DAGs is via `dag_dir_list_interval` config.
https://cwiki.apache.org/confluence/display/AIRFLOW/Scheduler+Basics#Configuration
You can set that config using `cluster.spec.config.airflow`
Set the env `AIRFLOW__SCHEDULER__ DAG_DIR_LIST_INTERVAL`
By default dags are refreshed every 5 minutes.
To enable continuous sync, use git or gcs dag source with once disabled.

```yaml
apiVersion: airflow.k8s.io/v1alpha1
kind: AirflowCluster
...
spec:
  ...
  config:
    airflow:
      AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL: 100 # default is 300s
  ...
  dags:
    subdir: ""
    gcs:
      bucket: "mydags"
  # OR
  dags:
    subdir: "airflow/example_dags/"
    git:
      repo: "https://github.com/apache/incubator-airflow/"
      once: false
```


