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



Security
========

.. include:: ../.github/SECURITY.rst

Web Authentication
------------------

By default, Airflow requires users to specify a password prior to login. You can use the
following CLI commands to create an account:

.. code-block:: bash

    # create an admin user
    airflow users create \
        --username admin \
        --firstname Peter \
        --lastname Parker \
        --role Admin \
        --email spiderman@superhero.org

It is however possible to switch on authentication by either using one of the supplied
backends or creating your own.

Be sure to checkout :doc:`api` for securing the API.

.. note::

   Airflow uses the config parser of Python. This config parser interpolates
   '%'-signs.  Make sure escape any ``%`` signs in your config file (but not
   environment variables) as ``%%``, otherwise Airflow might leak these
   passwords on a config parser exception to a log.

Password
''''''''

One of the simplest mechanisms for authentication is requiring users to specify a password before logging in.

Please use command line interface ``airflow users create`` to create accounts, or do that in the UI.

Other Methods
'''''''''''''

Standing on the shoulder of underlying framework Flask-AppBuilder, Airflow also supports authentication methods like
OAuth, OpenID, LDAP, REMOTE_USER. You can configure in ``webserver_config.py``. For details, please refer to
`Security section of FAB documentation <https://flask-appbuilder.readthedocs.io/en/latest/security.html>`_.


API Authentication
------------------

Authentication for the API is handled separately to the Web Authentication. The default is to
deny all requests:

.. code-block:: ini

    [api]
    auth_backend = airflow.api.auth.backend.deny_all

.. versionchanged:: 1.10.11

    In Airflow <1.10.11, the default setting was to allow all API requests without authentication, but this
    posed security risks for if the Webserver is publicly accessible.

If you wish to have the experimental API work, and aware of the risks of enabling this without authentication
(or if you have your own authentication layer in front of Airflow) you can set the following in ``airflow.cfg``:

.. code-block:: ini

    [api]
    auth_backend = airflow.api.auth.backend.default

Kerberos authentication is currently supported for the API.

To enable Kerberos authentication, set the following in the configuration:

.. code-block:: ini

    [api]
    auth_backend = airflow.api.auth.backend.kerberos_auth

    [kerberos]
    keytab = <KEYTAB>

The Kerberos service is configured as ``airflow/fully.qualified.domainname@REALM``. Make sure this
principal exists in the keytab file.

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

.. code-block:: ini

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

Using kerberos authentication
'''''''''''''''''''''''''''''

The hive hook has been updated to take advantage of kerberos authentication. To allow your DAGs to
use it, simply update the connection details with, for example:

.. code-block:: json

    { "use_beeline": true, "principal": "hive/_HOST@EXAMPLE.COM"}

Adjust the principal to your settings. The ``_HOST`` part will be replaced by the fully qualified domain name of
the server.

You can specify if you would like to use the dag owner as the user for the connection or the user specified in the login
section of the connection. For the login user, specify the following as extra:

.. code-block:: json

    { "use_beeline": true, "principal": "hive/_HOST@EXAMPLE.COM", "proxy_user": "login"}

For the DAG owner use:

.. code-block:: json

    { "use_beeline": true, "principal": "hive/_HOST@EXAMPLE.COM", "proxy_user": "owner"}

and in your DAG, when initializing the HiveOperator, specify:

.. code-block:: bash

    run_as_owner=True

To use kerberos authentication, you must install Airflow with the ``kerberos`` extras group:

.. code-block:: bash

   pip install 'apache-airflow[kerberos]'


SSL
---

SSL can be enabled by providing a certificate and key. Once enabled, be sure to use
"https://" in your browser.

.. code-block:: ini

    [webserver]
    web_server_ssl_cert = <path to cert>
    web_server_ssl_key = <path to key>

Enabling SSL will not automatically change the web server port. If you want to use the
standard port 443, you'll need to configure that too. Be aware that super user privileges
(or cap_net_bind_service on Linux) are required to listen on port 443.

.. code-block:: ini

    # Optionally, set the server to listen on the standard SSL port.
    web_server_port = 443
    base_url = http://<hostname or IP>:443

Enable CeleryExecutor with SSL. Ensure you properly generate client and server
certs and keys.

.. code-block:: ini

    [celery]
    ssl_active = True
    ssl_key = <path to key>
    ssl_cert = <path to cert>
    ssl_cacert = <path to cacert>

Rendering Airflow UI in a Web Frame from another site
------------------------------------------------------

Using Airflow in a web frame is enabled by default. To disable this (and prevent click jacking attacks)
set the below:

.. code-block:: ini

    [webserver]
    x_frame_enabled = False

Impersonation
-------------

Airflow has the ability to impersonate a unix user while running task
instances based on the task's ``run_as_user`` parameter, which takes a user's name.

**NOTE:** For impersonations to work, Airflow must be run with ``sudo`` as subtasks are run
with ``sudo -u`` and permissions of files are changed. Furthermore, the unix user needs to
exist on the worker. Here is what a simple sudoers file entry could look like to achieve
this, assuming as airflow is running as the ``airflow`` user. Note that this means that
the airflow user must be trusted and treated the same way as the root user.

.. code-block:: none

    airflow ALL=(ALL) NOPASSWD: ALL


Subtasks with impersonation will still log to the same folder, except that the files they
log to will have permissions changed such that only the unix user can write to it.

Default Impersonation
'''''''''''''''''''''
To prevent tasks that don't use impersonation to be run with ``sudo`` privileges, you can set the
``core:default_impersonation`` config which sets a default user impersonate if ``run_as_user`` is
not set.

.. code-block:: ini

    [core]
    default_impersonation = airflow


Flower Authentication
---------------------

Basic authentication for Celery Flower is supported.

You can specify the details either as an optional argument in the Flower process launching
command, or as a configuration item in your ``airflow.cfg``. For both cases, please provide
``user:password`` pairs separated by a comma.

.. code-block:: bash

    airflow flower --basic-auth=user1:password1,user2:password2

.. code-block:: ini

    [celery]
    flower_basic_auth = user1:password1,user2:password2


RBAC UI Security
----------------

Security of Airflow Webserver UI is handled by Flask AppBuilder (FAB).
Please read its related `security document <http://flask-appbuilder.readthedocs.io/en/latest/security.html>`_
regarding its security model.

Default Roles
'''''''''''''
Airflow ships with a set of roles by default: Admin, User, Op, Viewer, and Public.
Only ``Admin`` users could configure/alter the permissions for other roles. But it is not recommended
that ``Admin`` users alter these default roles in any way by removing
or adding permissions to these roles.

Admin
^^^^^
``Admin`` users have all possible permissions, including granting or revoking permissions from
other users.

Public
^^^^^^
``Public`` users (anonymous) don't have any permissions.

Viewer
^^^^^^
``Viewer`` users have limited viewer permissions

.. exampleinclude:: /../airflow/www/security.py
    :language: python
    :start-after: [START security_viewer_perms]
    :end-before: [END security_viewer_perms]

on limited web views

.. exampleinclude:: /../airflow/www/security.py
    :language: python
    :start-after: [START security_viewer_vms]
    :end-before: [END security_viewer_vms]


User
^^^^
``User`` users have ``Viewer`` permissions plus additional user permissions

.. exampleinclude:: /../airflow/www/security.py
    :language: python
    :start-after: [START security_user_perms]
    :end-before: [END security_user_perms]

on User web views which is the same as Viewer web views.

Op
^^
``Op`` users have ``User`` permissions plus additional op permissions

.. exampleinclude:: /../airflow/www/security.py
    :language: python
    :start-after: [START security_op_perms]
    :end-before: [END security_op_perms]

on ``User`` web views plus these additional op web views

.. exampleinclude:: /../airflow/www/security.py
    :language: python
    :start-after: [START security_op_vms]
    :end-before: [END security_op_vms]


Custom Roles
'''''''''''''

DAG Level Role
^^^^^^^^^^^^^^
``Admin`` can create a set of roles which are only allowed to view a certain set of dags. This is called DAG level access. Each dag defined in the dag model table
is treated as a ``View`` which has two permissions associated with it (``can_dag_read`` and ``can_dag_edit``). There is a special view called ``all_dags`` which
allows the role to access all the dags. The default ``Admin``, ``Viewer``, ``User``, ``Op`` roles can all access ``all_dags`` view.


.. _security/fernet:

Securing Connections
--------------------

Airflow uses `Fernet <https://github.com/fernet/spec/>`__ to encrypt passwords in the connection
configuration. It guarantees that a password encrypted using it cannot be manipulated or read without the key.
Fernet is an implementation of symmetric (also known as “secret key”) authenticated cryptography.

The first time Airflow is started, the ``airflow.cfg`` file is generated with the default configuration and the unique Fernet
key. The key is saved to option ``fernet_key`` of section ``[core]``.

You can also configure a fernet key using environment variables. This will overwrite the value from the
``airflow.cfg`` file

    .. code-block:: bash

      # Note the double underscores
      export AIRFLOW__CORE__FERNET_KEY=your_fernet_key

Generating fernet key
'''''''''''''''''''''

If you need to generate a new fernet key you can use the following code snippet.

    .. code-block:: python

      from cryptography.fernet import Fernet
      fernet_key= Fernet.generate_key()
      print(fernet_key.decode()) # your fernet_key, keep it in secured place!


Rotating encryption keys
''''''''''''''''''''''''

Once connection credentials and variables have been encrypted using a fernet
key, changing the key will cause decryption of existing credentials to fail. To
rotate the fernet key without invalidating existing encrypted values, prepend
the new key to the ``fernet_key`` setting, run
``airflow rotate_fernet_key``, and then drop the original key from
``fernet_keys``:

#. Set ``fernet_key`` to ``new_fernet_key,old_fernet_key``
#. Run ``airflow rotate_fernet_key`` to re-encrypt existing credentials with the new fernet key
#. Set ``fernet_key`` to ``new_fernet_key``

Sensitive Variable fields
-------------------------

By default, Airflow Value of a variable will be hidden if the key contains any words in
(‘password’, ‘secret’, ‘passwd’, ‘authorization’, ‘api_key’, ‘apikey’, ‘access_token’), but can be configured
to extend this list by using the following configurations option:

.. code-block:: ini

    [admin]
    hide_sensitive_variable_fields = comma_seperated_sensitive_variable_fields_list
