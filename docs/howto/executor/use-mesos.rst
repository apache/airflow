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

Scaling Out with Mesos (community contributed)
==============================================

There are two ways you can run airflow as a mesos framework:

1. Running airflow tasks directly on mesos slaves, requiring each mesos slave to have airflow installed and configured.
2. Running airflow tasks inside a docker container that has airflow installed, which is run on a mesos slave.

Tasks executed directly on mesos slaves
---------------------------------------

``MesosExecutor`` allows you to schedule airflow tasks on a Mesos cluster.
For this to work, you need a running mesos cluster and you must perform the following
steps -

1. Install airflow on a mesos slave where web server and scheduler will run,
   let's refer to this as the "Airflow server".
2. On the Airflow server, install mesos python eggs from `mesos downloads <http://open.mesosphere.com/downloads/mesos/>`_.
3. On the Airflow server, use a database (such as mysql) which can be accessed from all mesos
   slaves and add configuration in ``airflow.cfg``.
4. Change your ``airflow.cfg`` to point executor parameter to
   `MesosExecutor` and provide related Mesos settings.
5. On all mesos slaves, install airflow. Copy the ``airflow.cfg`` from
   Airflow server (so that it uses same sql alchemy connection).
6. On all mesos slaves, run the following for serving logs:

.. code-block:: bash

    airflow serve_logs

7. On Airflow server, to start processing/scheduling DAGs on mesos, run:

.. code-block:: bash

    airflow scheduler -p

Note: We need -p parameter to pickle the DAGs.

You can now see the airflow framework and corresponding tasks in mesos UI.
The logs for airflow tasks can be seen in airflow UI as usual.

For more information about mesos, refer to `mesos documentation <http://mesos.apache.org/documentation/latest/>`_.
For any queries/bugs on `MesosExecutor`, please contact `@kapil-malik <https://github.com/kapil-malik>`_.

Tasks executed in containers on mesos slaves
--------------------------------------------

`This gist <https://gist.github.com/sebradloff/f158874e615bda0005c6f4577b20036e>`_ contains all files and configuration changes necessary to achieve the following:

1. Create a dockerized version of airflow with mesos python eggs installed.

  We recommend taking advantage of docker's multi stage builds in order to achieve this. We have one Dockerfile that defines building a specific version of mesos from source (Dockerfile-mesos), in order to create the python eggs. In the airflow Dockerfile (Dockerfile-airflow) we copy the python eggs from the mesos image.

2. Create a mesos configuration block within the ``airflow.cfg``.

  The configuration block remains the same as the default airflow configuration (default_airflow.cfg), but has the addition of an option ``docker_image_slave``. This should be set to the name of the image you would like mesos to use when running airflow tasks. Make sure you have the proper configuration of the DNS record for your mesos master and any sort of authorization if any exists.

3. Change your ``airflow.cfg`` to point the executor parameter to
   `MesosExecutor` (`executor = SequentialExecutor`).

4. Make sure your mesos slave has access to the docker repository you are using for your ``docker_image_slave``.

  `Instructions are available in the mesos docs. <https://mesos.readthedocs.io/en/latest/docker-containerizer/#private-docker-repository>`_

The rest is up to you and how you want to work with a dockerized airflow configuration.
