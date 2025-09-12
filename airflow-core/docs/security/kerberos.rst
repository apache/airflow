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

Kerberos
--------

Airflow has initial support for Kerberos. This means that Airflow can renew Kerberos
tickets for itself and store it in the ticket cache. The hooks and Dags can make use of ticket
to authenticate against kerberized services.

Limitations
'''''''''''

Please note that at this time, not all hooks have been adjusted to make use of this functionality.
Also it does not integrate Kerberos into the web interface and you will have to rely on network
level security for now to make sure your service remains secure.

Celery integration has not been tried and tested yet. However, if you generate a key tab for every
host and launch a ticket renewer next to every worker it will most likely work.

Enabling Kerberos
'''''''''''''''''

Airflow
^^^^^^^

To enable Kerberos you will need to generate a (service) key tab.

.. code-block:: bash

    # in the kadmin.local or kadmin shell, create the airflow principal
    kadmin:  addprinc -randkey airflow/fully.qualified.domain.name@YOUR-REALM.COM

    # Create the Airflow keytab file that will contain the Airflow principal
    kadmin:  xst -norandkey -k airflow.keytab airflow/fully.qualified.domain.name

Now store this file in a location where the Airflow user can read it (chmod 600). And then add the following to
your ``airflow.cfg``

.. code-block:: ini

    [core]
    security = kerberos

    [kerberos]
    keytab = /etc/airflow/airflow.keytab
    reinit_frequency = 3600
    principal = airflow

In case you are using Airflow in a docker container based environment,
you can set the below environment variables in the ``Dockerfile`` instead of modifying ``airflow.cfg``

.. code-block:: dockerfile

    ENV AIRFLOW__CORE__SECURITY kerberos
    ENV AIRFLOW__KERBEROS__KEYTAB /etc/airflow/airflow.keytab
    ENV AIRFLOW__KERBEROS__INCLUDE_IP False


If you need more granular options for your Kerberos ticket the following options are available with the following default values:

.. code-block:: ini

    [kerberos]
    # Location of your ccache file once kinit has been performed
    ccache = /tmp/airflow_krb5_ccache
    # principal gets augmented with fqdn
    principal = airflow
    reinit_frequency = 3600
    kinit_path = kinit
    keytab = airflow.keytab

    # Allow kerberos token to be flag forwardable or not
    forwardable = True

    # Allow to include or remove local IP from kerberos token.
    # This is particularly useful if you use Airflow inside a VM NATted behind host system IP.
    include_ip = True

Keep in mind that Kerberos ticket are generated via ``kinit`` and will your use your local ``krb5.conf`` by default.

Launch the ticket renewer by

.. code-block:: bash

    # run ticket renewer
    airflow kerberos

To support more advanced deployment models for using kerberos in standard or one-time fashion,
you can specify the mode while running the ``airflow kerberos`` by using the ``--one-time`` flag.

a) standard: The Airflow kerberos command will run endlessly. The ticket renewer process runs continuously every few seconds
and refreshes the ticket if it has expired.
b) one-time: The Airflow kerberos will run once and exit. In case of failure the main task won't spin up.

The default mode is standard.

Example usages:

For standard mode:

.. code-block:: bash

    airflow kerberos

For one time mode:

.. code-block:: bash

    airflow kerberos --one-time

Hadoop
^^^^^^

If want to use impersonation this needs to be enabled in ``core-site.xml`` of your hadoop config.

.. code-block:: xml

    <property>
      <name>hadoop.proxyuser.airflow.groups</name>
      <value>*</value>
    </property>

    <property>
      <name>hadoop.proxyuser.airflow.users</name>
      <value>*</value>
    </property>

    <property>
      <name>hadoop.proxyuser.airflow.hosts</name>
      <value>*</value>
    </property>

Of course if you need to tighten your security replace the asterisk with something more appropriate.

Using Kerberos authentication
'''''''''''''''''''''''''''''

The Hive hook has been updated to take advantage of Kerberos authentication. To allow your Dags to
use it, simply update the connection details with, for example:

.. code-block:: json

    { "use_beeline": true, "principal": "hive/_HOST@EXAMPLE.COM"}

Adjust the principal to your settings. The ``_HOST`` part will be replaced by the fully qualified domain name of
the server.

You can specify if you would like to use the Dag owner as the user for the connection or the user specified in the login
section of the connection. For the login user, specify the following as extra:

.. code-block:: json

    { "use_beeline": true, "principal": "hive/_HOST@EXAMPLE.COM", "proxy_user": "login"}

For the Dag owner use:

.. code-block:: json

    { "use_beeline": true, "principal": "hive/_HOST@EXAMPLE.COM", "proxy_user": "owner"}

and in your Dag, when initializing the HiveOperator, specify:

.. code-block:: bash

    run_as_owner=True

To use kerberos authentication, you must install Airflow with the ``kerberos`` extras group:

.. code-block:: bash

   pip install 'apache-airflow[kerberos]'

You can read about some production aspects of Kerberos deployment at :ref:`production-deployment:kerberos`
