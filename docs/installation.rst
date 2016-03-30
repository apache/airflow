Installation
------------

Getting Airflow
'''''''''''''''

The easiest way to install the latest stable version of Airflow is with ``pip``:

.. code-block:: bash

    pip install airflow

You can also install Airflow with support for extra features like ``s3`` or ``postgres``:

.. code-block:: bash

    pip install "airflow[s3, postgres]"

Extra Packages
''''''''''''''

The ``airflow`` PyPI basic package only installs what's needed to get started.
Subpackages can be installed depending on what will be useful in your
environment. For instance, if you don't need connectivity with Postgres,
you won't have to go through the trouble of installing the ``postgres-devel``
yum package, or whatever equivalent applies on the distribution you are using.

Behind the scenes, Airflow does conditional imports of operators that require
these extra dependencies.

Here's the list of the subpackages and what they enable:

+---------------+-------------------------------------+-------------------------------------------------+
| subpackage    |     install command                 | enables                                         |
+===============+=====================================+=================================================+
|  all          | ``pip install airflow[all]``        | All Airflow features known to man               |
+---------------+-------------------------------------+-------------------------------------------------+
|  all_dbs      | ``pip install airflow[all_dbs]``    | All databases integrations                      |
+---------------+-------------------------------------+-------------------------------------------------+
|  async        | ``pip install airflow[async]``      | Async worker classes for gunicorn               |
+---------------+-------------------------------------+-------------------------------------------------+
|  devel        | ``pip install airflow[devel]``      | Minimum dev tools requirements                  |
+---------------+-------------------------------------+-------------------------------------------------+
|  devel_hadoop |``pip install airflow[devel_hadoop]``| Airflow + dependencies on the Hadoop stack      |
+---------------+-------------------------------------+-------------------------------------------------+
|  celery       | ``pip install airflow[celery]``     | CeleryExecutor                                  |
+---------------+-------------------------------------+-------------------------------------------------+
|  crypto       | ``pip install airflow[crypto]``     | Encrypt connection passwords in metadata db     |
+---------------+-------------------------------------+-------------------------------------------------+
|  druid        | ``pip install airflow[druid]``      | Druid.io related operators & hooks              |
+---------------+-------------------------------------+-------------------------------------------------+
|  gcp_api      | ``pip install airflow[gcp_api]``    | Google Cloud Platform hooks and operators       |
|               |                                     | (using ``google-api-python-client``)            |
+---------------+-------------------------------------+-------------------------------------------------+
|  gcloud       | ``pip install airflow[gcloud]``     | Google Cloud Platform hooks                     |
|               |                                     | (using ``gcloud``;                              |
|               |                                     | see ``/airflow/contrib/hooks/gcloud/readme.md``)|
+---------------+-------------------------------------+-------------------------------------------------+
|  jdbc         | ``pip install airflow[jdbc]``       | JDBC hooks and operators                        |
+---------------+-------------------------------------+-------------------------------------------------+
|  hdfs         | ``pip install airflow[hdfs]``       | HDFS hooks and operators                        |
+---------------+-------------------------------------+-------------------------------------------------+
|  hive         | ``pip install airflow[hive]``       | All Hive related operators                      |
+---------------+-------------------------------------+-------------------------------------------------+
|  kerberos     | ``pip install airflow[kerberos]``   | kerberos integration for kerberized hadoop      |
+---------------+-------------------------------------+-------------------------------------------------+
|  ldap         | ``pip install airflow[ldap]``       | ldap authentication for users                   |
+---------------+-------------------------------------+-------------------------------------------------+
|  mssql        | ``pip install airflow[mssql]``      | Microsoft SQL operators and hook,               |
|               |                                     | support as an Airflow backend                   |
+---------------+-------------------------------------+-------------------------------------------------+
|  mysql        | ``pip install airflow[mysql]``      | MySQL operators and hook, support as            |
|               |                                     | an Airflow backend                              |
+---------------+-------------------------------------+-------------------------------------------------+
|  password     | ``pip install airflow[password]``   | Password Authentication for users               |
+---------------+-------------------------------------+-------------------------------------------------+
|  postgres     | ``pip install airflow[postgres]``   | Postgres operators and hook, support            |
|               |                                     | as an Airflow backend                           |
+---------------+-------------------------------------+-------------------------------------------------+
|  qds          | ``pip install airflow[qds]``        | Enable QDS (qubole data services) support       |
+---------------+-------------------------------------+-------------------------------------------------+
|  rabbitmq     | ``pip install airflow[rabbitmq]``   | Rabbitmq support as a Celery backend            |
+---------------+-------------------------------------+-------------------------------------------------+
|  s3           | ``pip install airflow[s3]``         | ``S3KeySensor``, ``S3PrefixSensor``             |
+---------------+-------------------------------------+-------------------------------------------------+
|  samba        | ``pip install airflow[samba]``      | ``Hive2SambaOperator``                          |
+---------------+-------------------------------------+-------------------------------------------------+
|  slack        | ``pip install airflow[slack]``      | ``SlackAPIPostOperator``                        |
+---------------+-------------------------------------+-------------------------------------------------+
|  vertica      | ``pip install airflow[vertica]``    | Vertica hook                                    |
|               |                                     | support as an Airflow backend                   |
+---------------+-------------------------------------+-------------------------------------------------+
