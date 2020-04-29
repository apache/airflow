# docker-compose-airflow
Docker Compose Apache Airflow (Official Docker Images) with CeleryExecutor, InitDB and InitUser

Ideal for local development or small scale personal deployments.

### How to deploy:

**Step 1:** Clone this Repo and open terminal

**Step 2:** Go through .env file, init_airflow_setup.sh, docker-compose.yml file to change settings according to your preference. Or you can just keep them as it is for local development.

**Step 3:** Run `docker-compose up -d`

**Step 4:** Run `sh init_airflow_setup.sh` (Run this only for initial deployment, Airflow container will be in restart mode till this script successfully executed.)

**Step 5:** Go to http://localhost:8080 and login with user: _airflow_test_user_ and password: _airflow_test_password_ as specified in init_airflow_setup.sh script

**Step 6:** Enable and Run few dags and monitor Celery workers at http://localhost:5555


Airflow Environment variables are maintained in .env file where you can modify Airflow Config as specified in the link https://airflow.apache.org/docs/stable/howto/set-config.html

You can modify Executor modes by setting value of .env variable - AIRFLOW__CORE__EXECUTOR=CeleryExecutor

If you have existing airflow db, you can connect to it by setting .env variable - AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://postgres:postgres@postgres:5432/airflow
(If you are using an existing Airflow DB. Do NOT run init_airflow_setup.sh)

