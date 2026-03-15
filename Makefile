test-flowrate:
	PYTHONPATH=airflow-core/src:task-sdk/src:devel-common/src pytest \
		airflow-core/tests/unit/plugins/flowrate/ \
		--without-db-init --no-db-cleanup