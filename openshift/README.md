### Installation

This folder provides OpenShift template for Airflow cluster installation.
The next cluster structure will be provided:
  - Webserver and Scheduler same pod. Shared volume attached to **/usr/local/airflow/dags**. This way dags are synchronized between this containers.

#### Creation

To create cluster run next command:

        oc process -f airflow.template.yml  | oc create -f - 

**Important!** Before an installation start, please, put you variables into the template or you have to point them as CMD args.
Variables PROJECT_NAME and APPLICATION_NAME are usuallyu not the same. A project can have multiple applications.


#### Deletion

Delete cluster with the next command:

        oc delete secret fernet flower-auth postgresql redis && oc delete is airflow-base-image && oc delete all -l app=seism

#### Caveats

Fernet-key is a base64 encoded password required by scheduler. If you would like to change password, use next command to generate it:

        import base64
        from cryptography.fernet import Fernet


        FERNET_KEY = Fernet.generate_key().decode()
        FERNET_KEY = base64.b64decode(FERNET_KEY)

Accordingly to the template OpenShift will generate webserver and scheduler on different pods. You have to upload you dag file in **dags**
folders on these containers to make them executable.


#### Nice To See. TODO.

Rsync dags folders of webserver and scheduler.
Installation is not persistant now. Enable persistance.


#### Disclaimer.

Template is written by one person, could not be optimal in your case.
Modifications are welcomed. 

       
