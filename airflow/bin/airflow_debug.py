from airflow import configuration as conf, settings
from airflow.www.app import create_app

app, _ = create_app(conf)

app.run(debug=True)
