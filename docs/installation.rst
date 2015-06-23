Installation
------------
Setting up the sandbox from the :doc:`start` section was easy, now
working towards a production grade environment is a bit more work.

Note that Airflow is only
tested under Python 2.7.* as many of our dependencies don't support
python3 (as of 2015-06).

Extra Packages
''''''''''''''
The ``airflow`` PyPI basic package only installs what's needed to get started.
Subpackages can be installed depending on what will be useful in your 
environment. For instance, if you don't need connectivity with Postgres,
you won't have to go through the trouble of install the ``postgres-devel`` yum
package, or whatever equivalent on the distribution you are using.

Behind the scene, we do conditional imports on operators that require
these extra dependencies.

Here's the list of the subpackages and that they enable:

+-------------+------------------------------------+---------------------------------------+
| subpackage  |     install command                | enables                               |
+=============+====================================+=======================================+
|  mysql      |  ``pip install airflow[mysql]``    | MySQL operators and hook, support as  | 
|             |                                    | an Airflow backend                    |
+-------------+------------------------------------+---------------------------------------+
|  postgres   |  ``pip install airflow[postgres]`` | Postgres operators and hook, support  | 
|             |                                    | as an Airflow backend                 |
+-------------+------------------------------------+---------------------------------------+
|  samba      |  ``pip install airflow[samba]``    | ``Hive2SambaOperator``                |
+-------------+------------------------------------+---------------------------------------+
|  s3         | ``pip install airflow[s3]``        | ``S3KeySensor``, ``S3PrefixSensor``   |
+-------------+------------------------------------+---------------------------------------+
|  all        | ``pip install airflow[all]``       | All Airflow features known to man     |
+-------------+------------------------------------+---------------------------------------+


Setting up a Backend
''''''''''''''''''''
If you want to take a real test drive of Airflow, you should consider 
setting up a real database backend and switching to the LocalExecutor.

As Airflow was built to interact with its metadata using the great SqlAlchemy
library, you should be able to use any database backend supported as a
SqlAlchemy backend. We recommend using **MySQL** or **Postgres**.

Once you've setup your database to host Airflow, you'll need to alter the
SqlAlchemy connection string located in your configuration file
``$AIRFLOW_HOME/airflow.cfg``. You should then also change the "executor" 
setting to use "LocalExecutor", an executor that can parallelize task
instances locally.

.. code-block:: bash

    # initialize the database
    airflow initdb

Connections
'''''''''''
Airflow needs to know how to connect to your environment. Information 
such as hostname, port, login and password to other systems and services is
handled ``Admin->Connection`` section of the UI. The pipeline code you will 
author will reference the 'conn_id' of the Connection objects.

.. image:: img/connections.png


Scaling Out
'''''''''''
CeleryExecutor is the way you can scale out the number of workers. For this
to work, you need to setup a Celery backend (**RabbitMQ**, **Redis**, ...) and
change your ``airflow.cfg`` to point the executor parameter to 
CeleryExecutor and provide the related Celery settings.

For more information about setting up a Celery broker, refer to the
exhaustive `Celery documentation on the topic <http://docs.celeryproject.org/en/latest/getting-started/brokers/index.html>`_.

To kick off a worker, you need to setup Airflow and quick off the worker 
subcommand

.. code-block:: bash

    airflow worker

Your worker should start picking up tasks as soon as they get fired up in
its direction.

Note that you can also run "Celery Flower" a web UI build on top of Celery
to monitor your workers.


Web Authentication
''''''''''''''''''

By default, all gates are opened. An easy way to restrict access
to the web application is to do it at the network level, or by using
ssh tunnels.

However, it is possible to switch on 
authentication and define exactly how your users should login
into your Airflow environment. Airflow uses ``flask_login`` and
exposes a set of hooks in the ``airflow.default_login`` module. You can
alter the content of this module by overriding it as a ``airflow_login``
module. To do this, you would typically copy/paste ``airflow.default_login``
in a ``airflow_login.py`` and put it directly in your ``PYTHONPATH``.
