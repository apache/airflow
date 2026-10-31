from airflow import settings
from airflow.models import DagModel
from airflow.utils import timezone
from flask import Blueprint, render_template

dags_blueprint = Blueprint('dags', __name__, template_folder='templates')

@dags_blueprint.route('/dags')
def dags():
    # Get all DAGs
    dags = DagModel.query.all()

    # Get the status of all previous DAG runs
    previous_runs = []
    for dag in dags:
        runs = DagModel.query.filter_by(dag_id=dag.dag_id).order_by(DagModel.run_id.desc()).all()
        previous_runs.append({
            'dag_id': dag.dag_id,
            'status': 'success' if runs[0].state == 'success' else 'failure'
        })

    return render_template('dags.html', dags=dags, previous_runs=previous_runs)