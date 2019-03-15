TAG=$1

if [ -z "$TAG" ]; then
    echo "TAG is needed"
    echo "Usage: ./local_dev <Docker TAG>"
    exit 1
fi

REPO=$2
if [ -z "$REPO" ]; then
    REPO="internal.1.10.0"
    PORT=8080
else
    PORT=8081
fi

SCRIPT_DIR="$( cd "$( dirname "$0" )" && pwd )"
PROJ_ROOT_DIR="$SCRIPT_DIR/../../"

image_name=$REPO/`whoami`:$TAG
dockerfilename="Dockerfile.$REPO"
dockerfilepath=$PROJ_ROOT_DIR/_infra/development/dockerfiles/$dockerfilename

cd $PROJ_ROOT_DIR
echo `pwd`

has_image=`docker images $image_name | grep -v REPOSITORY`

if [ -z "$has_image" ]; then
  docker build -t $image_name -f $dockerfilepath .
fi

echo "image_name:" $image_name
echo "dockerfilepath:" $dockerfilepath

remote_airflow_dev="/mnt/airlab/repos/airflow_remote_dev/${REPO}"

mysql_cmd="mysql -e \"CREATE USER 'airflow'@'localhost' IDENTIFIED BY 'airflow';\" && mysql -e \"GRANT ALL PRIVILEGES ON *.\"*\" TO 'airflow'@'localhost' WITH GRANT OPTION;\""

docker_cmd="sudo echo '[mysqld]' >> /etc/mysql/my.cnf; sudo echo 'explicit_defaults_for_timestamp = 1' >> /etc/mysql/my.cnf; sudo service mysql start; "
docker_cmd="$docker_cmd export C_FORCE_ROOT=true; export AIRFLOW_HOME=/root/airflow; ./_infra/scripts/setup_ci.sh > /dev/null; $mysql_cmd && pip install --no-deps -q -e file:///airflow/#egg=apache-airflow && airflow initdb"

airflow_create_admin="airflow users -c --username admin --firstname Workflow --lastname Orc --role Admin --email spiderman@airbnb.com --password changeisgood"
docker_cmd="$docker_cmd && $airflow_create_admin ; bash"

echo "xxxxxxxxxxxxxxxxx docker cmd xxxxxxxxxxxxxxxxxxxxxx"
echo $docker_cmd
echo "AIRFLOW_HOME: " $remote_airflow_dev
echo "xxxxxxxxxxxxxxxxx docker cmd xxxxxxxxxxxxxxxxxxxxxx"

docker run -t -i -v $remote_airflow_dev:/root/airflow -v `pwd`:/airflow/ -w /airflow/ -p $PORT:8080 -e SLUGIFY_USES_TEXT_UNIDECODE=yes $image_name bash -c "$docker_cmd"

