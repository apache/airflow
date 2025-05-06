# Apache Airflow Provider for Apache SeaTunnel

This package provides an Apache Airflow provider for [Apache SeaTunnel](https://seatunnel.apache.org/), a high-performance, distributed data integration tool that supports real-time synchronization of massive data.

## Installation

You can install this provider package using pip:

```bash
cd /path/to/apache-airflow-provider-apache-seatunnel
pip install -e .
```

## Features

This provider package includes:

* A `SeaTunnelHook` for connecting to Apache SeaTunnel services
* A `SeaTunnelOperator` for running SeaTunnel jobs
* A `SeaTunnelJobSensor` for monitoring the status of SeaTunnel jobs (works with Zeta engine only)

## Usage

### Connection

First, create a connection in the Airflow UI with the following parameters:

* **Connection Id**: A unique identifier for your connection (e.g., `seatunnel_default`)
* **Connection Type**: `Apache SeaTunnel`
* **Host**: Hostname or IP address of the SeaTunnel server (e.g., `localhost`)
* **Port**: Port of the SeaTunnel server (e.g., `8083` for the Zeta engine REST API)
* **Extra**: JSON-encoded dictionary with additional configurations:
  ```json
  {
    "seatunnel_home": "/path/to/seatunnel"
  }
  ```

### Running a SeaTunnel Job

You can use the `SeaTunnelOperator` to run a SeaTunnel job:

```python
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow_seatunnel_provider.operators.seatunnel_operator import SeaTunnelOperator

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG(
    'seatunnel_example',
    default_args=default_args,
    schedule=None,
) as dag:
    
    # Run a SeaTunnel job using a configuration file
    seatunnel_job = SeaTunnelOperator(
        task_id='seatunnel_job',
        config_file='/path/to/your/config.conf',
        engine='zeta',  # 'flink', 'spark', or 'zeta'
        seatunnel_conn_id='seatunnel_default',
    )
    
    # Or provide configuration content directly
    seatunnel_job_inline = SeaTunnelOperator(
        task_id='seatunnel_job_inline',
        config_content="""
env {
  execution.parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    result_table_name = "fake"
    field_name = "name,age"
  }
}

transform {
  sql {
    sql = "select name,age from fake"
  }
}

sink {
  ConsoleSink {}
}
        """,
        engine='zeta',
        seatunnel_conn_id='seatunnel_default',
    )
```

### Monitoring a SeaTunnel Job (Zeta Engine Only)

You can use the `SeaTunnelJobSensor` to monitor the status of a SeaTunnel job:

```python
from airflow_seatunnel_provider.sensors.seatunnel_sensor import SeaTunnelJobSensor

# Monitor a SeaTunnel job
monitor_job = SeaTunnelJobSensor(
    task_id='monitor_job',
    job_id='job_id_to_monitor',
    target_states=['FINISHED'],
    seatunnel_conn_id='seatunnel_default',
    poke_interval=30,  # Check every 30 seconds
    timeout=3600,  # Timeout after 1 hour
)
```

## License

Apache License 2.0 