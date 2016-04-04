# Updating Airflow

This file aims to document the backwards-incompatible changes in Airflow and
assist people with migrating to a new version.

## 1.7 to 1.8

### DAGs now don't start automatically when created

To retain the old behavior, add this to your configuration:

```
dags_are_paused_at_creation = False
```

