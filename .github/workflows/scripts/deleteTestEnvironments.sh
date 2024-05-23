#!/usr/bin/env bash

set -e

###################################################################################################
# copied from
# https://unix.stackexchange.com/questions/65618/bash-script-wait-for-processes-and-get-return-code
declare -A JOBS

## run command in the background
background() {
  eval $1 & JOBS[$!]="$1"
}

## check exit status of each job
## preserve exit status in ${JOBS}
## returns 1 if any job failed
reap() {
  local cmd
  local status=0
  for pid in ${!JOBS[@]}; do
    cmd=${JOBS[${pid}]}
    wait ${pid} ; JOBS[${pid}]=$?
    if [[ ${JOBS[${pid}]} -ne 0 ]]; then
      status=${JOBS[${pid}]}
      echo -e "[${pid}] Exited with status: ${status}\n${cmd}"
    fi
  done
  return ${status}
}
###################################################################################################

deleteRegularEnv() {
  curl -s -L --request DELETE "https://api.clearscape.teradata.com/environments/$CSAE_ENV_NAME" \
    -H "Authorization: Bearer $CSAE_TOKEN"
}

deleteEnvWithQVCI() {
  curl -s -L --request DELETE "https://api.clearscape.teradata.com/environments/$CSAE_ENV_NAME-qvci" \
    -H "Authorization: Bearer $CSAE_TOKEN"
}

background deleteRegularEnv

reap
