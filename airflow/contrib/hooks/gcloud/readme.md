## GCP interfaces for Airflow

Please note: Airflow currently has two sets of hooks/operators for interacting with the Google Cloud Platform. At this time (March 2016), they are not compatible with each other due to reliance on different versions of the `oauth2client` library (which introduced breaking changes with version 2.0 in February 2016). Therefore, users have to install them explicitly.

##### The `gcp_api` package
The first set is based on a version of the [Google API Client](https://github.com/google/google-api-python-client) (`google-api-python-client`) that requires `oauth2client < 2.0`. It includes a large number of hooks/operators covering GCS, DataStore, and BigQuery.

To use this set, install `airflow[gcp_api]`.

##### The `gcloud` package
The second set is based on the [`gcloud` Python library](https://googlecloudplatform.github.io/gcloud-python/) and requires `oauth2client >= 2.0`. At this time it only includes a hook for GCS. Note that the hooks/operators in this set live in `gcloud/` directories, for clarity.

To use this set, install `airflow[gcloud]`.

##### Which should I use?
New users should probably build on the `gcloud` set because the `gcloud` library is the recommended way for Python apps to interact with the Google Cloud Platform. The interface is easier to extend than the API approach.

More pragmatically, if your existing code (hooks/operators/DAGs) depends on EITHER `gcloud >= 0.10` OR `google-api-python-client >= 1.5` (which both require `oauth2client >= 2.0`), then you won't be able to use `airflow[gcp_api]` due to compatibility issues.

However, if the hooks/operators in the `gcp_api` set meet your needs and you do not have other dependencies, then by all means use them!
