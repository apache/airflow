SCRIPT_PATH="performance_scripts/run_multiple_performance_tests.py"
PATH_TO_STUDY_FILE="performance_scripts/studies/composer2_dev.json"
PATH_TO_ELASTIC_DAG_CONFIG_FILE="performance_scripts/performance_dags/elastic_dag/elastic_dag_configurations/bigquery_insert_job_workflow.json"
CI_BUILD_ID="manual"
RESULTS_BUCKET="cloud-composer-performance-results"
PROJECT_ID="composer-performance-tests"
MAX_CONCURRENCY=50

python $SCRIPT_PATH \
--study-file-path $PATH_TO_STUDY_FILE \
--elastic-dag-config-file-path $PATH_TO_ELASTIC_DAG_CONFIG_FILE \
--script-user $USER \
--ci-build-id $CI_BUILD_ID \
--reports-bucket $RESULTS_BUCKET \
--reports-project-id $PROJECT_ID \
--max-concurrency $MAX_CONCURRENCY
