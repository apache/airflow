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

Web Authentication
------------------

Password
''''''''

.. note::

   This is for flask-admin based web UI only. If you are using FAB-based web UI with RBAC feature,
   please use command line interface ``create_user`` to create accounts, or do that in the FAB-based UI itself.

One of the simplest mechanisms for authentication is requiring users to specify a password before logging in.
Password authentication requires the used of the ``password`` subpackage in your requirements file. Password hashing
uses ``bcrypt`` before storing passwords.

.. code-block:: bash

    [webserver]
    authenticate = True
    auth_backend = airflow.contrib.auth.backends.password_auth

When password auth is enabled, an initial user credential will need to be created before anyone can login. An initial
user was not created in the migrations for this authentication backend to prevent default Airflow installations from
attack. Creating a new user has to be done via a Python REPL on the same machine Airflow is installed.

.. code-block:: bash

    # navigate to the airflow installation directory
    $ cd ~/airflow
    $ python
    Python 2.7.9 (default, Feb 10 2015, 03:28:08)
    Type "help", "copyright", "credits" or "license" for more information.
    >>> import airflow
    >>> from airflow import models, settings
    >>> from airflow.contrib.auth.backends.password_auth import PasswordUser
    >>> user = PasswordUser(models.User())
    >>> user.username = 'new_user_name'
    >>> user.email = 'new_user_email@example.com'
    >>> user.password = 'set_the_password'
    >>> session = settings.Session()
    >>> session.add(user)
    >>> session.commit()
    >>> session.close()
    >>> exit()

LDAP
''''

.. note::

   This is for flask-admin based web UI only. If you are using FAB-based web UI with RBAC feature,
   check the `Security section of FAB docs <https://flask-appbuilder.readthedocs.io/en/latest/security.html>`_
   for how to configure in ``webserver_config.py`` file.

To turn on LDAP authentication configure your ``airflow.cfg`` as follows. Please note that the example uses
an encrypted connection to the ldap server as we do not want passwords be readable on the network level.

Additionally, if you are using Active Directory, and are not explicitly specifying an OU that your users are in,
you will need to change ``search_scope`` to "SUBTREE".

Valid search_scope options can be found in the `ldap3 Documentation <http://ldap3.readthedocs.org/searches.html?highlight=search_scope>`_

.. code-block:: bash

    [webserver]
    authenticate = True
    auth_backend = airflow.contrib.auth.backends.ldap_auth

    [ldap]
    # set a connection without encryption: uri = ldap://<your.ldap.server>:<port>
    uri = ldaps://<your.ldap.server>:<port>
    user_filter = objectClass=*
    # in case of Active Directory you would use: user_name_attr = sAMAccountName
    user_name_attr = uid
    # group_member_attr should be set accordingly with *_filter
    # eg :
    #     group_member_attr = groupMembership
    #     superuser_filter = groupMembership=CN=airflow-super-users...
    group_member_attr = memberOf
    superuser_filter = memberOf=CN=airflow-super-users,OU=Groups,OU=RWC,OU=US,OU=NORAM,DC=example,DC=com
    data_profiler_filter = memberOf=CN=airflow-data-profilers,OU=Groups,OU=RWC,OU=US,OU=NORAM,DC=example,DC=com
    bind_user = cn=Manager,dc=example,dc=com
    bind_password = insecure
    basedn = dc=example,dc=com
    cacert = /etc/ca/ldap_ca.crt
    # Set search_scope to one of them:  BASE, LEVEL , SUBTREE
    # Set search_scope to SUBTREE if using Active Directory, and not specifying an Organizational Unit
    search_scope = LEVEL

    # This option tells ldap3 to ignore schemas that are considered malformed. This sometimes comes up
    # when using hosted ldap services.
    ignore_malformed_schema = False

The superuser_filter and data_profiler_filter are optional. If defined, these configurations allow you to specify LDAP groups that users must belong to in order to have superuser (admin) and data-profiler permissions. If undefined, all users will be superusers and data profilers.

Roll your own
'''''''''''''

Airflow uses ``flask_login`` and
exposes a set of hooks in the ``airflow.default_login`` module. You can
alter the content and make it part of the ``PYTHONPATH`` and configure it as a backend in ``airflow.cfg``.

.. code-block:: bash

    [webserver]
    authenticate = True
    auth_backend = mypackage.auth

Multi-tenancy
-------------

You can filter the list of dags in webserver by owner name when authentication
is turned on by setting ``webserver:filter_by_owner`` in your config. With this, a user will see
only the dags which it is owner of, unless it is a superuser.

.. code-block:: bash

    [webserver]
    filter_by_owner = True


API Authentication
------------------

Authentication for the API is handled separately to the Web Authentication. The default is to not
require any authentication on the API i.e. wide open by default. This is not recommended if your
Airflow webserver is publicly accessible, and you should probably use the ``deny all`` backend:

.. code-block:: ini

    [api]
    auth_backend = airflow.api.auth.backend.deny_all

Two "real" methods for authentication are currently supported for the API.

To enabled Password authentication, set the following in the configuration:

.. code-block:: ini

    [api]
    auth_backend = airflow.contrib.auth.backends.password_auth

It's usage is similar to the Password Authentication used for the Web interface.

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

Adjust the principal to your settings. The ``_HOST`` part will be replaced by the fully qualified domain name of
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

To use kerberos authentication, you must install Airflow with the ``kerberos`` extras group:

.. code-block:: bash

   pip install 'apache-airflow[kerberos]'

OAuth Authentication
--------------------

.. note::

   This is for flask-admin based web UI only. If you are using FAB-based web UI with RBAC feature,
   check the `Security section of FAB docs <https://flask-appbuilder.readthedocs.io/en/latest/security.html>`_
   for how to configure in ``webserver_config.py`` file.

GitHub Enterprise (GHE) Authentication
''''''''''''''''''''''''''''''''''''''

The GitHub Enterprise authentication backend can be used to authenticate users
against an installation of GitHub Enterprise using OAuth2. You can optionally
specify a team whitelist (composed of slug cased team names) to restrict login
to only members of those teams.

.. code-block:: bash

    [webserver]
    authenticate = True
    auth_backend = airflow.contrib.auth.backends.github_enterprise_auth

    [github_enterprise]
    host = github.example.com
    client_id = oauth_key_from_github_enterprise
    client_secret = oauth_secret_from_github_enterprise
    oauth_callback_route = /example/ghe_oauth/callback
    allowed_teams = 1, 345, 23

.. note:: If you do not specify a team whitelist, anyone with a valid account on
   your GHE installation will be able to login to Airflow.

To use GHE authentication, you must install Airflow with the ``github_enterprise`` extras group:

.. code-block:: bash

   pip install 'apache-airflow[github_enterprise]'

Setting up GHE Authentication
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

An application must be setup in GHE before you can use the GHE authentication
backend. In order to setup an application:

1. Navigate to your GHE profile
2. Select 'Applications' from the left hand nav
3. Select the 'Developer Applications' tab
4. Click 'Register new application'
5. Fill in the required information (the 'Authorization callback URL' must be fully qualified e.g. http://airflow.example.com/example/ghe_oauth/callback)
6. Click 'Register application'
7. Copy 'Client ID', 'Client Secret', and your callback route to your ``airflow.cfg`` according to the above example

Using GHE Authentication with github.com
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

It is possible to use GHE authentication with github.com:

1. `Create an OAuth App <https://developer.github.com/apps/building-oauth-apps/creating-an-oauth-app/>`_
2. Copy 'Client ID', 'Client Secret' to your airflow.cfg according to the above example
3. Set ``host = github.com`` and ``oauth_callback_route = /oauth/callback`` in ``airflow.cfg``

Google Authentication
'''''''''''''''''''''

The Google authentication backend can be used to authenticate users
against Google using OAuth2. You must specify the domains to restrict
login, separated with a comma, to only members of those domains.

.. code-block:: ini

    [webserver]
    authenticate = True
    auth_backend = airflow.contrib.auth.backends.google_auth

    [google]
    client_id = google_client_id
    client_secret = google_client_secret
    oauth_callback_route = /oauth2callback
    domain = example1.com,example2.com

To use Google authentication, you must install Airflow with the ``google_auth`` extras group:

.. code-block:: bash

   pip install 'apache-airflow[google_auth]'

Setting up Google Authentication
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

An application must be setup in the Google API Console before you can use the Google authentication
backend. In order to setup an application:

1. Navigate to https://console.developers.google.com/apis/
2. Select 'Credentials' from the left hand nav
3. Click 'Create credentials' and choose 'OAuth client ID'
4. Choose 'Web application'
5. Fill in the required information (the 'Authorized redirect URIs' must be fully qualified e.g. http://airflow.example.com/oauth2callback)
6. Click 'Create'
7. Copy 'Client ID', 'Client Secret', and your redirect URI to your ``airflow.cfg`` according to the above example

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

    airflow flower --basic_auth=user1:password1,user2:password2

.. code-block:: ini

    [celery]
    flower_basic_auth = user1:password1,user2:password2


RBAC UI Security
----------------

Security of Airflow Webserver UI when running with ``rbac=True`` in the config is handled by Flask AppBuilder (FAB).
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

.. code:: python

    VIEWER_PERMS = {
        'menu_access',
        'can_index',
        'can_list',
        'can_show',
        'can_chart',
        'can_dag_stats',
        'can_dag_details',
        'can_task_stats',
        'can_code',
        'can_log',
        'can_get_logs_with_metadata',
        'can_tries',
        'can_graph',
        'can_tree',
        'can_task',
        'can_task_instances',
        'can_xcom',
        'can_gantt',
        'can_landing_times',
        'can_duration',
        'can_blocked',
        'can_rendered',
        'can_pickle_info',
        'can_version',
    }

on limited web views

.. code:: python

    VIEWER_VMS = {
        'Airflow',
        'DagModelView',
        'Browse',
        'DAG Runs',
        'DagRunModelView',
        'Task Instances',
        'TaskInstanceModelView',
        'SLA Misses',
        'SlaMissModelView',
        'Jobs',
        'JobModelView',
        'Logs',
        'LogModelView',
        'Docs',
        'Documentation',
        'GitHub',
        'About',
        'Version',
        'VersionView',
    }

User
^^^^
``User`` users have ``Viewer`` permissions plus additional user permissions

.. code:: python

    USER_PERMS = {
        'can_dagrun_clear',
        'can_run',
        'can_trigger',
        'can_add',
        'can_edit',
        'can_delete',
        'can_paused',
        'can_refresh',
        'can_success',
        'muldelete',
        'set_failed',
        'set_running',
        'set_success',
        'clear',
        'can_clear',
    }


on User web views which is the same as Viewer web views.

Op
^^
``Op`` users have ``User`` permissions plus additional op permissions

.. code:: python

    OP_PERMS = {
        'can_conf',
        'can_varimport',
    }

on ``User`` web views plus these additional op web views

.. code:: python

    OP_VMS = {
        'Admin',
        'Configurations',
        'ConfigurationView',
        'Connections',
        'ConnectionModelView',
        'Pools',
        'PoolModelView',
        'Variables',
        'VariableModelView',
        'XComs',
        'XComModelView',
    }

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
