Knative Executor
================


Testing the Knative Executor
****************************

Step 1: Build an Airflow Docker Image
=====================================

For the initial version of the KnativeExecutor, we will only be offering the "baked-in" mode. Requiring baked-in mode means that you will need to include all of your DAGs in the Docker image built on top of airflow.

Step 2 (optional): Test using a normal Kubernetes Deployment
************************************************************

Before jumping into the complexities of Knative, we recommend you take your new Knative queue and test it without any form of autoscaling. Build a deployment with more workers than you would expect to need and attach a service to communicate. We offer an example service/deployment here:

.. code:: yaml

  foo:

Why we recommend using a deployment first
-----------------------------------------

Knative is a powerful system that allows for easy autoscaling, but it is not the easiest for debugging. We find that once the Knative services are stable, they work exceptionally well, but it is better to do initial debugging without worrying about additional infrastructure layers.


Step 3: Create a Knative Queue with a minimum of 1
**************************************************

Now that your service is ready, let's get started with Knative! For your initial queue, we recommend you set a minimum size to 1 using the ``autoscaling.knative.dev/minScale`` label. _It is also crucial that you set the ``containerConcurrency`` to the number of workers you plan to start up._ For more information on autoscaling see `knative autoscaling docs <https://knative.dev/docs/serving/configuring-autoscaling/>`_.

.. code:: yaml

  foo:

Why a minimum size of 1?
------------------------

When first launching a Knative service, we want to be able to debug any failures of the service pod. If you do not set a minimum size, there is a possibility that the pod will fail, Knative will kill the pod, and the service will then be unreachable. Having a ``minScale`` of 1 will force Knative to continue attempting to launch the failing service, making debugging easier.

Step 4: Use Knative to your heart's content!
********************************************

