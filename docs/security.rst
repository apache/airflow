Security
========

Web Authentication
------------------

By default, all gates are opened. An easy way to restrict access
to the web application is to do it at the network level, or by using
SSH tunnels.

It is however possible to switch on authentication by either using one of the supplied
backends or create your own.

Password
''''''''

One of the simplest mechanisms for authentication is requiring users to specify a password before logging in.
Password authentication requires the used of the ``password`` subpackage in your requirements file. Password hashing
uses bcrypt before storing passwords.

.. code-block:: bash

    [webserver]
    authenticate = True
    auth_backend = airflow.contrib.auth.backends.password_auth


LDAP
''''

To turn on LDAP authentication configure your ``airflow.cfg`` as follows. Please note that the example uses
an encrypted connection to the ldap server as you probably do not want passwords be readable on the network level.
It is however possible to configure without encryption if you really want to.

.. code-block:: bash

    [webserver]
    authenticate = True
    auth_backend = airflow.contrib.auth.backends.ldap_auth

    [ldap]
    uri = ldaps://<your.ldap.server>:<port>
    user_filter = objectClass=*
    user_name_attr = uid # in case of Active Directory you would use sAMAccountName
    bind_user = cn=Manager,dc=example,dc=com
    bind_password = insecure
    basedn = dc=example,dc=com
    cacert = /etc/ca/ldap_ca.crt


Roll your own
'''''''''''''

Airflow uses ``flask_login`` and
exposes a set of hooks in the ``airflow.default_login`` module. You can
alter the content and make it part of the ``PYTHONPATH`` and configure it as a backend in ``airflow.cfg```.

.. code-block:: bash

    [webserver]
    authenticate = True
    auth_backend = mypackage.auth

Multi-tenancy
-------------

You can filter the list of dags in webserver by owner name, when authentication
is turned on, by setting webserver.filter_by_owner as true in your ``airflow.cfg``
With this, when a user authenticates and logs into webserver, it will see only the dags
which it is owner of. A super_user, will be able to see all the dags although.
This makes the web UI a multi-tenant UI, where a user will only be able to see dags
created by itself.


Kerberos
--------
Airflow has initial support for Kerberos. This means that airflow can renew kerberos
tickets for itself and store it in the ticket cache. The hooks and dags can make use of ticket
to authenticate against kerberized services.

Limitations
'''''''''''

Please note that at this time not all hooks have been adjusted to make use of this functionality yet.
Also it does not integrate kerberos into the web interface and you will have to rely on network
level security for now to make sure your service remains secure.

Celery integration has not been tried and tested yet. However if you generate a key tab for every host
and launch a ticket renewer next to every worker it will most likely work.

Enabling kerberos
'''''''''''''''''

#### Airflow

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

#### Hadoop

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

The hive hook has been updated to take advantage of kerberos authentication. To allow your DAGs to use it simply
update the connection details with, for example:

.. code-block:: bash

    { "use_beeline": true, "principal": "hive/_HOST@EXAMPLE.COM"}

Adjust the principal to your settings. The _HOST part will be replaced by the fully qualified domain name of
the server.

You can specify if you would like to use the dag owner as the user for the connection or the user specified in the login
section of the connection. For the login user specify the following as extra:

.. code-block:: bash

    { "use_beeline": true, "principal": "hive/_HOST@EXAMPLE.COM", "proxy_user": "login"}

For the DAG owner use:

.. code-block:: bash

    { "use_beeline": true, "principal": "hive/_HOST@EXAMPLE.COM", "proxy_user": "owner"}

and in your DAG, when initializing the HiveOperator, specify

.. code-block:: bash

    run_as_owner=True

