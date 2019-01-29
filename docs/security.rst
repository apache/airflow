..  Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

..    http://www.apache.org/licenses/LICENSE-2.0

..  Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

Security
========

By default, all gates are opened. An easy way to restrict access
to the web application is to do it at the network level, or by using
SSH tunnels.

It is however possible to switch on authentication by either using one of the supplied
backends or creating your own.

Be sure to checkout :doc:`api` for securing the API.

.. note::

   Airflow uses the config parser of Python. This config parser interpolates
   '%'-signs.  Make sure escape any ``%`` signs in your config file (but not
   environment variables) as ``%%``, otherwise Airflow might leak these
   passwords on a config parser exception to a log.

Reporting Vulnerabilities
-------------------------

The Apache Software Foundation takes security issues very seriously. Apache
Airflow specifically offers security features and is responsive to issues
around its features. If you have any concern around Airflow Security or believe
you have uncovered a vulnerability, we suggest that you get in touch via the
e-mail address security@apache.org. In the message, try to provide a
description of the issue and ideally a way of reproducing it. The security team
will get back to you after assessing the description.

Note that this security address should be used only for undisclosed
vulnerabilities. Dealing with fixed issues or general questions on how to use
the security features should be handled regularly via the user and the dev
lists. Please report any security problems to the project security address
before disclosing it publicly.

The `ASF Security team's page <https://www.apache.org/security/>`_ describes
how vulnerability reports are handled, and includes PGP keys if you wish to use
that.

Web Authentication
------------------

Password
''''''''

One of the simplest mechanisms for authentication is requiring users to specify a password before logging in.

Please use command line interface ``airflow users --create`` to create accounts, or do that in the UI.

Kerberos
--------

Airflow has initial support for Kerberos. This means that airflow can renew kerberos
tickets for itself and store it in the ticket cache. The hooks and dags can make use of ticket
to authenticate against kerberized services.

Limitations
'''''''''''

Please note that at this time, not all hooks have been adjusted to make use of this functionality.
Also it does not integrate kerberos into the web interface and you will have to rely on network
level security for now to make sure your service remains secure.

Celery integration has not been tried and tested yet. However, if you generate a key tab for every
host and launch a ticket renewer next to every worker it will most likely work.

Enabling kerberos
'''''''''''''''''

Airflow
^^^^^^^

To enable kerberos you will need to generate a (service) key tab.

.. code-block:: bash

    # in the kadmin.local or kadmin shell, create the airflow principal
    kadmin:  addprinc -randkey airflow/fully.qualified.domain.name@YOUR-REALM.COM

    # Create the airflow keytab file that will contain the airflow principal
    kadmin:  xst -norandkey -k airflow.keytab airflow/fully.qualified.domain.name

Now store this file in a location where the airflow user can read it (chmod 600). And then add the following to
your ``airflow.cfg``

.. code-block:: bash

    [core]
    security = kerberos

    [kerberos]
    keytab = /etc/airflow/airflow.keytab
    reinit_frequency = 3600
    principal = airflow

Launch the ticket renewer by

.. code-block:: bash

    # run ticket renewer
    airflow kerberos

Hadoop
^^^^^^

If want to use impersonation this needs to be enabled in ``core-site.xml`` of your hadoop config.

.. code-block:: bash

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

Using kerberos authentication
'''''''''''''''''''''''''''''

The hive hook has been updated to take advantage of kerberos authentication. To allow your DAGs to
use it, simply update the connection details with, for example:

.. code-block:: bash

    { "use_beeline": true, "principal": "hive/_HOST@EXAMPLE.COM"}

Adjust the principal to your settings. The _HOST part will be replaced by the fully qualified domain name of
the server.

You can specify if you would like to use the dag owner as the user for the connection or the user specified in the login
section of the connection. For the login user, specify the following as extra:

.. code-block:: bash

    { "use_beeline": true, "principal": "hive/_HOST@EXAMPLE.COM", "proxy_user": "login"}

For the DAG owner use:

.. code-block:: bash

    { "use_beeline": true, "principal": "hive/_HOST@EXAMPLE.COM", "proxy_user": "owner"}

and in your DAG, when initializing the HiveOperator, specify:

.. code-block:: bash

    run_as_owner=True

To use kerberos authentication, you must install Airflow with the `kerberos` extras group:

.. code-block:: bash

   pip install apache-airflow[kerberos]


SSL
---

SSL can be enabled by providing a certificate and key. Once enabled, be sure to use
"https://" in your browser.

.. code-block:: bash

    [webserver]
    web_server_ssl_cert = <path to cert>
    web_server_ssl_key = <path to key>

Enabling SSL will not automatically change the web server port. If you want to use the
standard port 443, you'll need to configure that too. Be aware that super user privileges
(or cap_net_bind_service on Linux) are required to listen on port 443.

.. code-block:: bash

    # Optionally, set the server to listen on the standard SSL port.
    web_server_port = 443
    base_url = http://<hostname or IP>:443

Enable CeleryExecutor with SSL. Ensure you properly generate client and server
certs and keys.

.. code-block:: bash

    [celery]
    ssl_active = True
    ssl_key = <path to key>
    ssl_cert = <path to cert>
    ssl_cacert = <path to cacert>

Impersonation
-------------

Airflow has the ability to impersonate a unix user while running task
instances based on the task's ``run_as_user`` parameter, which takes a user's name.

**NOTE:** For impersonations to work, Airflow must be run with `sudo` as subtasks are run
with `sudo -u` and permissions of files are changed. Furthermore, the unix user needs to
exist on the worker. Here is what a simple sudoers file entry could look like to achieve
this, assuming as airflow is running as the `airflow` user. Note that this means that
the airflow user must be trusted and treated the same way as the root user.

.. code-block:: none

    airflow ALL=(ALL) NOPASSWD: ALL


Subtasks with impersonation will still log to the same folder, except that the files they
log to will have permissions changed such that only the unix user can write to it.

Default Impersonation
'''''''''''''''''''''
To prevent tasks that don't use impersonation to be run with `sudo` privileges, you can set the
``core:default_impersonation`` config which sets a default user impersonate if `run_as_user` is
not set.

.. code-block:: bash

    [core]
    default_impersonation = airflow


Flower Authentication
---------------------

Basic authentication for Celery Flower is supported.

You can specify the details either as an optional argument in the Flower process launching
command, or as a configuration item in your ``airflow.cfg``. For both cases, please provide
`user:password` pairs separated by a comma.

.. code-block:: bash

    airflow flower --basic_auth=user1:password1,user2:password2

.. code-block:: bash

    [celery]
    flower_basic_auth = user1:password1,user2:password2
