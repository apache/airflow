from airflow.operators import HiveToDruidTransfer
from airflow import DAG
from datetime import datetime

args = {
            'owner': 'qi_wang',
            'start_date': datetime(2015, 4, 4),
}

dag = DAG("test_druid", default_args=args)


HiveToDruidTransfer(task_id="load_dummy_test",
                    sql="select * from qi.druid_test_dataset_w_platform_1 \
                            limit 10;",
                    druid_datasource="airflow_test",
                    ts_dim="ds",
                    dag=dag
                )
