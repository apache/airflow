from datetime import datetime, timedelta

from airflow import DAG

from airflow_seatunnel_provider.operators.seatunnel_operator import SeaTunnelOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'seatunnel_example',
    default_args=default_args,
    description='A simple example DAG using SeaTunnel',
    schedule=None,
    start_date=datetime.now() - timedelta(days=1),
    tags=['example', 'seatunnel'],
) as dag:
    
    # Example 1: Run a job with a config file
    # Make sure to create this file or adjust the path
    seatunnel_task_file = SeaTunnelOperator(
        task_id='seatunnel_task_file',
        config_content='seatunnel/config/http2mysql.conf',
        engine='zeta',  # or 'flink', 'spark'
        seatunnel_conn_id='seatunnel_default',
    )
    
    # Example 2: Run a job with inline configuration
    # This is a simple job that uses FakeSource to generate data and ConsoleSink to print it
    seatunnel_task_inline = SeaTunnelOperator(
        task_id='seatunnel_task_inline',
        config_content="""
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  # This is a example source plugin **only for test and demonstrate the feature source plugin**
  FakeSource {
    plugin_output = "fake"
    parallelism = 1
    schema = {
      fields {
        name = "string"
        age = "int"
      }
    }
  }
}

transform {
}

sink {
  console {
    plugin_input="fake"
  }
}
        """,
        engine='zeta',
        seatunnel_conn_id='seatunnel_default',
    )
    
    # You can define dependencies between tasks
    seatunnel_task_file >> seatunnel_task_inline