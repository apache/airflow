#!/usr/bin/env bash

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
LDAP_DB=/tmp/ldap_db

echo "Creating database directory"

rm -rf ${LDAP_DB} && mkdir ${LDAP_DB} && cp  /usr/share/doc/slapd/examples/DB_CONFIG ${LDAP_DB}

echo "Launching OpenLDAP ..."

# Start slapd with non root privileges
slapd -h "ldap://127.0.0.1:3890/" -f ${DIR}/slapd.conf

ps aux

# Wait for LDAP to start
sleep 1