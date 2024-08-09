#!/usr/bin/env bash
set -euo pipefail

MY_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
README_FILE="${MY_DIR}/../../performance_scripts/README.md"

add_cli_help_to_readme(){
  CLI_HELP_COMMAND=$1
  DIRECTORY=$2
  LEAD=$3
  TAIL=$4

  TMP_FILE=$(mktemp)
  TMP_OUTPUT=$(mktemp)

  cd $DIRECTORY || exit;
  echo "\`\`\`" >"${TMP_FILE}"

  eval $CLI_HELP_COMMAND | sed 's/^/  /' | sed 's/ *$//' \
  >> "${TMP_FILE}"

  echo "\`\`\`" >> "${TMP_FILE}"

  MAX_LEN=$(awk '{ print length($0); }' "${TMP_FILE}" | sort -n | tail -1 )

  BEGIN_GEN=$(grep -n "${LEAD}" <"${README_FILE}" | sed 's/\(.*\):.*/\1/g')
  END_GEN=$(grep -n "${TAIL}" <"${README_FILE}" | sed 's/\(.*\):.*/\1/g')
  cat <(head -n "${BEGIN_GEN}" "${README_FILE}") \
      "${TMP_FILE}" \
      <(tail -n +"${END_GEN}" "${README_FILE}") \
      >"${TMP_OUTPUT}"

  mv "${TMP_OUTPUT}" "${README_FILE}"
}

add_cli_help_to_readme \
'airflow_gepard -h' \
"${MY_DIR}/../../" \
'^<!-- AIRFLOW_GEPARD_AUTO_START -->$' \
'^<!-- AIRFLOW_GEPARD_AUTO_END -->'

add_cli_help_to_readme \
'python3 run_multiple_performance_tests.py -h' \
"${MY_DIR}/../../performance_scripts" \
'^<!-- MULTIPLE_RUNS_AUTO_START -->$' \
'^<!-- MULTIPLE_RUNS_AUTO_END -->'
