Kubernetes Executor
===================

The kubernetes executor is introduced in Apache Airflow 1.10.0. The Kubernetes executor will create a new pod for every task instance.

Example helm charts are available at `scripts/ci/kubernetes/kube/{airflow,volumes,postgres}.yaml` in the source distribution. The volumes are optional and depend on your configuration. There are two volumes available:
- Dags: by storing all the dags onto the persistent disks, all the workers can read the dags from there. Another option is using git-sync, before starting the container, a git pull of the dags repository will be performed and used throughout the lifecycle of the pod/
- Logs: by storing the logs onto a persistent disk, all the logs will be available for all the workers and the webserver itself. If you don't configure this, the logs will be lost after the worker pods shuts down. Another option is to use S3/GCS/etc to store the logs.


Kubernetes Operator
===================



.. code:: python

    from airflow.comtrib.operators import KubernetesOperator
    k = KubernetesPodOperator(namespace='default',
                              image="ubuntu:16.04",
                              cmds=["bash", "-cx"],
                              arguments=["echo", "10"],
                              labels={"foo": "bar"},
                              name="test",
                              task_id="task"
                              )



=================================   ====================================
Variable                            Description
=================================   ====================================
``@namespace``                      The namespace is your isolated work environment within kubernetes
``@image``                          docker image you wish to launch. Defaults to dockerhub.io, but fully qualified URLS will point to custom repositories
``@cmds``                           To start a task in a docker image, we need to tell it what to do. the cmds array is the space seperated bash command that will define the task completed by the container
``arguments``                       arguments for your bash command
``@labels``                         Labels are an important element of launching kubernetes pods, as it tells kubernetes what pods a service can route to. For example, if you launch 5 postgres pods with the label  {'postgres':'foo'} and create a postgres service with the same label, kubernetes will know that any time that service is queried, it can pick any of those 5 postgres instances as the endpoint for that service.
``@name``                           name of the task you want to run, will be used to generate a pod id
=================================   ====================================
