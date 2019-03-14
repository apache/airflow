#!/usr/bin/env bash

mysqld &

# psql for psql hooks
sudo service postgresql start
sudo -u postgres createuser root
sudo -u postgres createdb airflow
psql_version=`ls /etc/postgresql`
echo "local   all             all                                     trust" > /etc/postgresql/$psql_version/main/pg_hba.conf
echo "local   all             all                                     trust" >> /etc/postgresql/$psql_version/main/pg_hba.conf
echo "host    all             all             127.0.0.1/32            trust" >> /etc/postgresql/$psql_version/main/pg_hba.conf
echo "host    all             all             ::1/128                 trust" >> /etc/postgresql/$psql_version/main/pg_hba.conf
sudo service postgresql restart
# end psql https://gist.github.com/p1nox/4953113

# redis for celery
redis-server --daemonize yes

mysql -e "create database airflow"

# ssh, for ssh hook, ftp hook, sftp hook
ssh-keygen -t rsa -N "" -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub > ~/.ssh/authorized_keys
chmod 600 ~/.ssh/*
sudo echo "Subsystem sftp internal-sftp" > /etc/ssh/sshd_config
sudo /etc/init.d/ssh restart
# end ssh

# xxxxxxxxxxxxxxx  LDAP xxxxxxxxxxxxxxxxxxx
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
DATA_DIR="${DIR}/data"
FIXTURES_DIR="$DIR/ldif"
LDAP_DB=/tmp/ldap_db

echo "Creating database directory"

rm -rf ${LDAP_DB} && mkdir ${LDAP_DB} && cp  /usr/share/doc/slapd/examples/DB_CONFIG ${LDAP_DB}

echo "Launching OpenLDAP ..."

slapd -h "ldap://127.0.0.1:3890/" -f ${DATA_DIR}/slapd.conf

# Wait for LDAP to start
sleep 10
# xxxxxxxxxxxxxxx  LDAP xxxxxxxxxxxxxxxxxxx

# xxxxxxxxxxxxxxx  Load data xxxxxxxxxxxxxxxxxxx
DATA_FILE="${DATA_DIR}/baby_names.csv"
DATABASE=airflow_ci
mysqladmin -u root create ${DATABASE}
mysql -u root < ${DATA_DIR}/mysql_schema.sql
mysqlimport --local -u root --fields-optionally-enclosed-by="\"" --fields-terminated-by=, --ignore-lines=1 ${DATABASE} ${DATA_FILE}
# Load data

LOAD_ORDER=("example.com.ldif" "manager.example.com.ldif" "users.example.com.ldif" "groups.example.com.ldif")

load_fixture () {
  ldapadd -x -H ldap://127.0.0.1:3890/ -D "cn=Manager,dc=example,dc=com" -w insecure -f $1
}

for FILE in "${LOAD_ORDER[@]}"
do
  load_fixture "${FIXTURES_DIR}/${FILE}"
done;
# xxxxxxxxxxxxxxx  Load data xxxxxxxxxxxxxxxxxxx


cd /tmp/
nohup java -cp "/tmp/minicluster-1.1-SNAPSHOT/*" com.ing.minicluster.MiniCluster &> /dev/null &
