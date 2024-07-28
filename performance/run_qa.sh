SCRIPT_PATH="src/run_multiple_performance_tests.py"
PATH_TO_STUDY_FILE="src/studies/composer3_qa.json"
PATH_TO_ELASTIC_DAG_CONFIG_FILE="src/performance_dags/elastic_dag/elastic_dag_configurations/tiny_workflow.json"
CI_BUILD_ID="manual"
MAX_CONCURRENCY=50

python $SCRIPT_PATH \
--study-file-path $PATH_TO_STUDY_FILE \
--elastic-dag-config-file-path $PATH_TO_ELASTIC_DAG_CONFIG_FILE \
--script-user $USER \
--ci-build-id $CI_BUILD_ID \
--max-concurrency $MAX_CONCURRENCY
