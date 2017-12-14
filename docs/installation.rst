Installation
------------

Getting Airflow
'''''''''''''''

The easiest way to install the latest stable version of Airflow is with ``pip``:

.. code-block:: bash

    pip install apache-airflow

You can also install Airflow with support for extra features like ``s3`` or ``postgres``:

.. code-block:: bash

    pip install "apache-airflow[s3, postgres]"

Extra Packages
''''''''''''''

The ``apache-airflow`` PyPI basic package only installs what's needed to get started.
Subpackages can be installed depending on what will be useful in your
environment. For instance, if you don't need connectivity with Postgres,
you won't have to go through the trouble of installing the ``postgres-devel``
yum package, or whatever equivalent applies on the distribution you are using.

Behind the scenes, Airflow does conditional imports of operators that require
these extra dependencies.

Here's the list of the subpackages and what they enable:

+---------------+----------------------------------------------+-------------------------------------------------+
| subpackage    | install command                              | enables                                         |
+===============+==============================================+=================================================+
|  all          | ``pip install apache-airflow[all]``          | All Airflow features known to man               |
+---------------+----------------------------------------------+-------------------------------------------------+
|  all_dbs      | ``pip install apache-airflow[all_dbs]``      | All databases integrations                      |
+---------------+----------------------------------------------+-------------------------------------------------+
|  async        | ``pip install apache-airflow[async]``        | Async worker classes for gunicorn               |
+---------------+----------------------------------------------+-------------------------------------------------+
|  devel        | ``pip install apache-airflow[devel]``        | Minimum dev tools requirements                  |
+---------------+----------------------------------------------+-------------------------------------------------+
|  devel_hadoop | ``pip install apache-airflow[devel_hadoop]`` | Airflow + dependencies on the Hadoop stack      |
+---------------+----------------------------------------------+-------------------------------------------------+
|  celery       | ``pip install apache-airflow[celery]``       | CeleryExecutor                                  |
+---------------+----------------------------------------------+-------------------------------------------------+
|  crypto       | ``pip install apache-airflow[crypto]``       | Encrypt connection passwords in metadata db     |
+---------------+----------------------------------------------+-------------------------------------------------+
|  druid        | ``pip install apache-airflow[druid]``        | Druid.io related operators & hooks              |
+---------------+----------------------------------------------+-------------------------------------------------+
|  gcp_api      | ``pip install apache-airflow[gcp_api]``      | Google Cloud Platform hooks and operators       |
|               |                                              | (using ``google-api-python-client``)            |
+---------------+----------------------------------------------+-------------------------------------------------+
|  jdbc         | ``pip install apache-airflow[jdbc]``         | JDBC hooks and operators                        |
+---------------+----------------------------------------------+-------------------------------------------------+
|  hdfs         | ``pip install apache-airflow[hdfs]``         | HDFS hooks and operators                        |
+---------------+----------------------------------------------+-------------------------------------------------+
|  hive         | ``pip install apache-airflow[hive]``         | All Hive related operators                      |
+---------------+----------------------------------------------+-------------------------------------------------+
|  kerberos     | ``pip install apache-airflow[kerberos]``     | kerberos integration for kerberized hadoop      |
+---------------+----------------------------------------------+-------------------------------------------------+
|  ldap         | ``pip install apache-airflow[ldap]``         | ldap authentication for users                   |
+---------------+----------------------------------------------+-------------------------------------------------+
|  mssql        | ``pip install apache-airflow[mssql]``        | Microsoft SQL operators and hook,               |
|               |                                              | support as an Airflow backend                   |
+---------------+----------------------------------------------+-------------------------------------------------+
|  mysql        | ``pip install apache-airflow[mysql]``        | MySQL operators and hook, support as            |
|               |                                              | an Airflow backend. The version of MySQL server |
|               |                                              | has to be 5.6.4+. The exact version upper bound | 
|               |                                              | depends on version of ``mysqlclient`` package.  |
|               |                                              | For example, ``mysqlclient`` 1.3.12 can only be |
|               |                                              | used with MySQL server 5.6.4 through 5.7.       |
+---------------+----------------------------------------------+-------------------------------------------------+
|  password     | ``pip install apache-airflow[password]``     | Password Authentication for users               |
+---------------+----------------------------------------------+-------------------------------------------------+
|  postgres     | ``pip install apache-airflow[postgres]``     | Postgres operators and hook, support            |
|               |                                              | as an Airflow backend                           |
+---------------+----------------------------------------------+-------------------------------------------------+
|  qds          | ``pip install apache-airflow[qds]``          | Enable QDS (qubole data services) support       |
+---------------+----------------------------------------------+-------------------------------------------------+
|  rabbitmq     | ``pip install apache-airflow[rabbitmq]``     | Rabbitmq support as a Celery backend            |
+---------------+----------------------------------------------+-------------------------------------------------+
|  s3           | ``pip install apache-airflow[s3]``           | ``S3KeySensor``, ``S3PrefixSensor``             |
+---------------+----------------------------------------------+-------------------------------------------------+
|  samba        | ``pip install apache-airflow[samba]``        | ``Hive2SambaOperator``                          |
+---------------+----------------------------------------------+-------------------------------------------------+
|  slack        | ``pip install apache-airflow[slack]``        | ``SlackAPIPostOperator``                        |
+---------------+----------------------------------------------+-------------------------------------------------+
|  vertica      | ``pip install apache-airflow[vertica]``      | Vertica hook                                    |
|               |                                              | support as an Airflow backend                   |
+---------------+----------------------------------------------+-------------------------------------------------+
|  cloudant     | ``pip install apache-airflow[cloudant]``     | Cloudant hook                                   |
+---------------+----------------------------------------------+-------------------------------------------------+
|  redis        | ``pip install apache-airflow[redis]``        | Redis hooks and sensors                         |
+---------------+----------------------------------------------+-------------------------------------------------+
