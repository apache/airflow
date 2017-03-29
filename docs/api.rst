Experimental Rest API
=====================

Airflow exposes an experimental Rest API. It is available through the webserver. Endpoints are
available at /api/experimental/. Please note that we expect the endpoint definitions to change.

Endpoints
---------

This is a place holder until the swagger definitions are active

* /api/experimental/dags/<DAG_ID>/tasks/<TASK_ID> returns info for a task (GET).
* /api/experimental/dags/<DAG_ID>/dag_runs creates a dag_run for a given dag id (POST).

CLI
-----

For some functions the cli can use the API. To configure the CLI to use the API when available
configure as follows:

.. code-block:: bash

    [cli]
    api_client = airflow.api.client.json_client
    endpoint_url = http://<WEBSERVER>:<PORT>


Authentication
--------------

Only Kerberos authentication is currently supported for the API. To enable this set the following
in the configuration:

.. code-block:: bash

    [api]
    auth_backend = airflow.api.auth.backend.default

    [kerberos]
    keytab = <KEYTAB>

The Kerberos service is configured as `airflow/fully.qualified.domainname@REALM`. Make sure this
principal exists in the keytab file.
