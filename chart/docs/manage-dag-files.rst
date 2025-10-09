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


Manage Dag files
================

When you create new or modify existing Dag files, it is necessary to deploy them into the environment. This section will describe some basic techniques you can use.

Bake Dags in docker image
-------------------------

With this approach, you include your Dag files and related code in the Airflow image.

This method requires redeploying the services in the helm chart with the new docker image in order to deploy the new Dag code. This can work well particularly if Dag code is not expected to change frequently.

.. code-block:: bash

    docker build --pull --tag "my-company/airflow:8a0da78" . -f - <<EOF
    FROM apache/airflow

    COPY ./dags/ \${AIRFLOW_HOME}/dags/

    EOF

.. note::

   In Airflow images prior to version 2.0.2, there was a bug that required you to use
   a bit longer Dockerfile, to make sure the image remains OpenShift-compatible (i.e Dag
   has root group similarly as other files). In 2.0.2 this has been fixed.

.. code-block:: bash

    docker build --pull --tag "my-company/airflow:8a0da78" . -f - <<EOF
    FROM apache/airflow:2.0.2

    USER root

    COPY --chown=airflow:root ./dags/ \${AIRFLOW_HOME}/dags/

    USER airflow

    EOF


Then publish it in the accessible registry:

.. code-block:: bash

    docker push my-company/airflow:8a0da78

Finally, update the Airflow pods with that image:

.. code-block:: bash

    helm upgrade --install airflow apache-airflow/airflow \
      --set images.airflow.repository=my-company/airflow \
      --set images.airflow.tag=8a0da78

If you are deploying an image with a constant tag, you need to make sure that the image is pulled every time.

.. warning::

    Using constant tag should be used only for testing/development purpose. It is a bad practice to use the same tag as you'll lose the history of your code.

.. code-block:: bash

    helm upgrade --install airflow apache-airflow/airflow \
      --set images.airflow.repository=my-company/airflow \
      --set images.airflow.tag=8a0da78 \
      --set images.airflow.pullPolicy=Always \
      --set airflowPodAnnotations.random=r$(uuidgen)

The randomly generated pod annotation will ensure that pods are refreshed on helm upgrade.

If you are deploying an image from a private repository, you need to create a secret, e.g. ``gitlab-registry-credentials`` (refer `Pull an Image from a Private Registry <https://kubernetes.io/docs/tasks/configure-pod-container/pull-image-private-registry/>`_ for details), and specify it using ``--set registry.secretName``:


.. code-block:: bash

    helm upgrade --install airflow apache-airflow/airflow \
      --set images.airflow.repository=my-company/airflow \
      --set images.airflow.tag=8a0da78 \
      --set images.airflow.pullPolicy=Always \
      --set registry.secretName=gitlab-registry-credentials

Using git-sync
--------------

Mounting Dags using git-sync sidecar with persistence enabled
.............................................................

This option will use a Persistent Volume Claim with an access mode of ``ReadWriteMany``.
The scheduler pod will sync Dags from a git repository onto the PVC every configured number of
seconds. The other pods will read the synced Dags. Not all volume plugins have support for
``ReadWriteMany`` access mode.
Refer `Persistent Volume Access Modes <https://kubernetes.io/docs/concepts/storage/persistent-volumes/#access-modes>`__
for details.

.. code-block:: bash

    helm upgrade --install airflow apache-airflow/airflow \
      --set dags.persistence.enabled=true \
      --set dags.gitSync.enabled=true
      # you can also override the other persistence or gitSync values
      # by setting the  dags.persistence.* and dags.gitSync.* values
      # Please refer to values.yaml for details


Mounting Dags using git-sync sidecar without persistence
........................................................

This option will use an always running Git-Sync sidecar on every scheduler, webserver (if ``airflowVersion < 2.0.0``)
and worker pods.
The Git-Sync sidecar containers will sync Dags from a git repository every configured number of
seconds. If you are using the ``KubernetesExecutor``, Git-sync will run as an init container on your worker pods.

.. code-block:: bash

    helm upgrade --install airflow apache-airflow/airflow \
      --set dags.persistence.enabled=false \
      --set dags.gitSync.enabled=true
      # you can also override the other gitSync values
      # by setting the dags.gitSync.* values
      # Refer values.yaml for details

When using ``apache-airflow >= 2.0.0``, :ref:`Dag Serialization <apache-airflow:dag-serialization>` is enabled by default,
hence Webserver does not need access to Dag files, so ``git-sync`` sidecar is not run on Webserver.

Notes for combining git-sync and persistence
............................................

While using both git-sync and persistence for Dags is possible, it is generally not recommended unless the
deployment manager carefully considered the trade-offs it brings. There are cases when git-sync without
persistence has other trade-offs (for example delays in synchronization of Dags vs. rate-limiting of Git
servers) that can often be mitigated (for example by sending signals to git-sync containers via web-hooks
when new commits are pushed to the repository) but there might be cases where you still might want to choose
git-sync and Persistence together, but as a Deployment Manager you should be aware of some consequences it has.

git-sync solution is primarily designed to be used for local, POSIX-compliant volumes to checkout Git
repositories into. Part of the process of synchronization of commits from git-sync involves checking out
new version of files in a freshly created folder and swapping symbolic links to the new folder, after the
checkout is complete. This is done to ensure that the whole Dags folder is consistent at all times. The way
git-sync works with symbolic-link swaps, makes sure that Parsing the Dags always work on a consistent
(single-commit-based) set of files in the whole Dag folder.

This approach, however might have undesirable side effects when the folder that git-sync works on is not
a local volume, but is a persistent volume (so effectively a networked, distributed volume). Depending on
the technology behind the persistent volumes might handle git-sync approach differently and with non-obvious
consequences. There are a lot of persistence solutions available for various K8S installations and each of
them has different characteristics, so you need to carefully test and monitor your filesystem to make sure
those undesired side effects do not affect you. Those effects might change over time or depend on parameters
like how often the files are being scanned by the Dag File Processor, the number and complexity of your
Dags, how remote and how distributed your persistent volumes are, how many IOPS you allocate for some of
the filesystem (usually highly paid feature of such filesystems is how many IOPS you can get) and many other
factors.

The way git-sync works with symbolic links swapping generally causes a linear growth of the throughput and
potential delays in synchronization. The networking traffic from checkouts comes in bursts and the bursts
are linearly proportional to the number and size of files you have in the repository, makes it vulnerable
to pretty sudden and unexpected demand increase. Most of the persistence solution work "good enough" for
smaller/shorter burst of traffic, but when they outgrow certain thresholds, you need to upgrade the
networking to a much more capable and expensive options. This is difficult to control and impossible to
mitigate, so you might be suddenly faced with situation to pay a lot more for IOPS/persistence option to
keep your Dags sufficiently synchronized to avoid inconsistencies and delays in synchronization.

The side-effects that you might observe:

* burst of networking/communication at the moment when new commit is checked out (because of the quick
  succession of deleting old files, creating new files, symbolic link swapping.
* temporary lack of consistency between files in Dag folders while Dags are being synced (because of delays
  in distributing changes to individual files for various nodes in the cluster)
* visible drops of performance of the persistence solution when your Dag number grows, drops that might
  amplify the side effects described above.
* some of persistence solutions might lack filesystem functionality that git-sync needs to perform the sync
  (for example changing permissions or creating symbolic links). While those can often be mitigated it is
  only recommended to use git-sync with fully POSIX-filesystem compliant persistence filesystems.

General recommendation to use git-sync with local volumes only, and if you want to also use persistence, you
need to make sure that the persistence solution you use is POSIX-compliant and you monitor the side-effects
it might have.

Synchronizing multiple Git repositories with git-sync
.....................................................

Airflow git-sync integration in the Helm Chart, does not allow to configure multiple repositories to be
synchronized at the same time. The Dag folder must come from single git repository. However it is possible
to use `submodules <https://git-scm.com/book/en/v2/Git-Tools-Submodules>`_ to create an "umbrella" repository
that you can use to bring a number of git repositories checked out together (with ``--submodules recursive``
option). There are success stories of Airflow users using such approach with 100s of repositories put
together as submodules via such "umbrella" repo approach. When you choose this solution, however,
you need to work out the way how to link the submodules, when to update the umbrella repo when "submodule"
repository change and work out versioning approach and automate it. This might be as simple as always
using latest versions of all the submodule repositories, or as complex as managing versioning of shared
libraries, Dags and code across multiple teams and doing that following your release process.

An example of such complex approach can found in this
`Manage Dags at scale <https://s.apache.org/airflow-manage-dags-at-scale>`_ presentation from the Airflow
Summit.


Mounting Dags from an externally populated PVC
----------------------------------------------

In this approach, Airflow will read the Dags from a PVC which has ``ReadOnlyMany`` or ``ReadWriteMany`` access mode. You will have to ensure that the PVC is populated/updated with the required Dags (this won't be handled by the chart). You pass in the name of the volume claim to the chart:

.. code-block:: bash

    helm upgrade --install airflow apache-airflow/airflow \
      --set dags.persistence.enabled=true \
      --set dags.persistence.existingClaim=my-volume-claim \
      --set dags.gitSync.enabled=false

Mounting Dags from a private GitHub repo using Git-Sync sidecar
---------------------------------------------------------------
Create a private repo on GitHub if you have not created one already.

Then create your ssh keys:

.. code-block:: bash

    ssh-keygen -t rsa -b 4096 -C "your_email@example.com"

Add the public key to your private repo (under ``Settings > Deploy keys``).

You have to convert the private ssh key to a base64 string. You can convert the private ssh key file like so:

.. code-block:: bash

    base64 <my-private-ssh-key> -w 0 > temp.txt

Then copy the string from the ``temp.txt`` file. You'll add it to your ``override-values.yaml`` next.

In this example, you will create a yaml file called ``override-values.yaml`` to override values in the
``values.yaml`` file, instead of using ``--set``:

.. code-block:: yaml

    dags:
      gitSync:
        enabled: true
        repo: git@github.com:<username>/<private-repo-name>.git
        branch: <branch-name>
        subPath: ""
        sshKeySecret: airflow-ssh-secret
    extraSecrets:
      airflow-ssh-secret:
        data: |
          gitSshKey: '<base64-converted-ssh-private-key>'

Don't forget to copy in your private key base64 string.

Finally, from the context of your Airflow Helm chart directory, you can install Airflow:

.. code-block:: bash

    helm upgrade --install airflow apache-airflow/airflow -f override-values.yaml

If you have done everything correctly, Git-Sync will pick up the changes you make to the Dags
in your private GitHub repo.

You should take this a step further and set ``dags.gitSync.knownHosts`` so you are not susceptible to man-in-the-middle
attacks. This process is documented in the :ref:`production guide <production-guide:knownhosts>`.
