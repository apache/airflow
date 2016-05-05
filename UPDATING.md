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

#### Operators no longer accept arbitrary arguments
Previously, `Operator.__init__()` accepted any arguments (either positional `*args` or keyword `**kwargs`) without complaint. Now, invalid arguments will be rejected.
