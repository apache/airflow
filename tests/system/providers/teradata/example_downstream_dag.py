from __future__ import annotations

from airflow import DAG
from airflow.providers.teradata.operators.teradata import TeradataOperator, TeradataStoredProcedureOperator

CONN_ID = "teradata_sp_call"

with DAG(
    dag_id="example_downstream_dag",
    max_active_runs=1,
    max_active_tasks=3,
    catchup=False,
    render_template_as_native_obj=True,
) as dag:
    t1 = TeradataOperator(
        task_id="t1",
        conn_id=CONN_ID,
        sql=r"""replace procedure examplestoredproc (in p1 integer, out p2 integer)
             begin
                set p2 = p1 + p1 ;
             end ;
            """,
    )

    t2 = TeradataStoredProcedureOperator(
        task_id="t2",
        conn_id=CONN_ID,
        procedure="examplestoredproc",
        parameters=["{{ dag_run.conf.get('input_param1') }}", int],
    )

    t1 >> t2

