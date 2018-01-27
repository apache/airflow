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

Authentication for the API is handled separately to the Web Authentication. The default is to not
require any authentication on the API -- i.e. wide open by default. This is not recommended if your
Airflow webserver is publicly accessible, and you should probably use the deny all backend:

.. code-block:: ini

    [api]
    auth_backend = airflow.api.auth.backend.deny_all


Kerberos is the only "real" authentication mechanism currently supported for the API. To enable
this set the following in the configuration:

.. code-block:: ini

    [api]
    auth_backend = airflow.api.auth.backend.kerberos_auth

    [kerberos]
    keytab = <KEYTAB>

The Kerberos service is configured as ``airflow/fully.qualified.domainname@REALM``. Make sure this
principal exists in the keytab file.
