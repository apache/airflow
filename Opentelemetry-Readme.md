# Documentation

This contains documentation for implementing Opentelemetry with Apache Airflow project

### Running Opentelemetry in development

First clone the airflow repository. See: Airflow-GitHub <https://github.com/apache/airflow>

Once you have airflow set-up locally, you can simply get opentelemetry traces by adding the '--integration jaeger' flag

`./breeze start-airflow --integration jaeger`

Then view your traces at `localhost:16686`
