### Installation

This folder provides OpenShift template for Airflow cluster installation.
The next cluster structure will be provided:
  - Webserver and Scheduler same pod. Shared volume attached to **/usr/local/airflow/dags**. This way dags are synchronized between this webserver and scheduler containers.
  - PostgresSQL (if it is already present in your project please comment it in the template, and update secret values)
  - Redis
  - Worker (1)
  - Flower

#### Creation

To create cluster run next command:

        oc process -f airflow.template.yml  | oc create -f - 

#### Important Template Variables
Before an installation start, please, put your variables into the template or you have to point them as CMD args.
  1. PROJECT_NAME
  2. APPLICATION_NAME A project can have multiple applications.


#### Deletion

Delete cluster with the next command:

        oc delete secret fernet flower-auth postgresql redis && oc delete is airflow-base-image && oc delete all -l app=seism

#### Caveats

Fernet-key is a base64 encoded password required by scheduler. If you would like to change password, use next command to generate it:

        import base64
        from cryptography.fernet import Fernet


        FERNET_KEY = Fernet.generate_key().decode()
        FERNET_KEY = base64.b64decode(FERNET_KEY)

Accordingly to the template OpenShift will generate scheduler and flower on different pods. You have to update airflow.cfg file to set proper flower uri:

        # Celery Flower is a sweet UI for Celery. Airflow has a shortcut to start
        # it `airflow flower`. This defines the IP that Celery Flower runs on
        flower_host = flower


#### Nice To See. TODO.
airflow.cfg needs to be patched automatically.
