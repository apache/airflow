#!/usr/bin/env bash
set -o verbose

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
FIXTURES_DIR="$DIR/ldif"

load_fixture () {
  ldapadd -x -H ldap://127.0.0.1:3890/ -D "cn=Manager,dc=example,dc=com" -w insecure -f $1
}

for FIXTURE in `ls ${FIXTURES_DIR}`
do
  load_fixture "${FIXTURES_DIR}/${FIXTURE}"
done;
