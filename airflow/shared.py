from airflow.models import DagBag
from airflow import configuration as conf
import os

dagbag = DagBag(os.path.expanduser(conf.get('core', 'DAGS_FOLDER')))
