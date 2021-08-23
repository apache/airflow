import datetime
import json

from plugins.utils.logger import generate_logger
import os
from airflow.models import DAG, DagRun
from datetime import timedelta
from airflow.operators.python_operator import PythonOperator
import datetime as dt
import pendulum
from plugins.utils.utils import get_craft_type, \
    generate_bolt_number, get_curve_params, get_result, get_curve, trigger_push_result_to_mq, get_curve_mode, \
    should_trigger_training

RUNTIME_ENV = os.environ.get('RUNTIME_ENV', 'dev')
MAX_ACTIVE_TRAINING = os.environ.get('MAX_ACTIVE_TRAINING', 100)

if RUNTIME_ENV == 'prod':
    schedule_interval = None
else:
    schedule_interval = None

_logger = generate_logger(__name__)

DAG_ID = 'curve_training_dag'
TASK_ID = 'curve_training_task'


def onDagFail(context):
    _logger.error("{0} Run Fail".format(context))


def onDagSuccess(context):
    _logger.info("{0} Run Success".format(context))


local_tz = pendulum.timezone("Asia/Shanghai")

desoutter_default_args = {
    'owner': 'desoutter',
    'depends_on_past': False,
    'start_date': dt.datetime(2020, 1, 1, tzinfo=local_tz),
    'email': ['support@desoutter.cn'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 4,
    'retry_delay': timedelta(minutes=2),
    'on_failure_callback': onDagFail,
    'on_success_callback': onDagSuccess,
    'on_retry_callback': None,
    'trigger_rule': 'all_success'
}


def update_confirm_data(verify_error, curve_mode):
    # data = task_data.get('task', {})
    data = {}
    data.update({
        "curve_mode": curve_mode,
        "verify_error": verify_error
    })
    return {
        'task': data
    }


def do_trigger_training(result, final_state):
    entity_id = result.get('entity_id')
    _logger.info('getting curve...')
    curve = get_curve(entity_id)
    curve_mode = get_curve_mode(final_state, result.get('error_tag'))
    task_param = update_confirm_data(
        result.get('verify_error'),
        curve_mode
    )
    controller_name = result.get('controller_name', None)
    job = result.get('job', None)
    batch_count = result.get('batch_count', None)
    pset = result.get('pset', None)
    bolt_number = generate_bolt_number(controller_name, job, batch_count, pset)
    curve_params = get_curve_params(bolt_number)
    for k, v in result.items():
        if isinstance(v, datetime.datetime):
            result.update({
                k: v.strftime("%Y-%m-%d %H:%M:%S")
            })
    data = {
        'entity_id': entity_id,
        'result': result,
        'curve': curve,
        'craft_type': get_craft_type(bolt_number)
    }
    data.update(task_param)
    data.update(curve_params)
    from plugins.cas.cas_plugin import CasHook
    cas = CasHook(role='training')
    cas.trigger_training(data)


def verify_params(**kwargs):
    dag_run = kwargs.get('dag_run', None)
    params = None
    if isinstance(dag_run, DagRun):
        params = getattr(dag_run, 'conf')
    if isinstance(dag_run, dict):
        params = dag_run.get('conf', None)
    if params is None:
        raise Exception(u'参数conf不存在')
    final_state = params.get('final_state')
    entity_id = params.get('entity_id')
    error_tags = params.get('error_tags', [])
    if not final_state:
        raise Exception('empty final_state')
    return entity_id, final_state, error_tags


def trigger_training_task(task_instance, **kwargs):
    """触发训练任务"""
    entity_id, final_state, error_tags = verify_params(**kwargs)
    result = get_result(entity_id)

    if should_trigger_training(result.get('result'), final_state, result.get('error_tag'), error_tags):
        _logger.info('trigger training...')
        do_trigger_training(result, final_state)
        _logger.info('training finished, saving error tag')
    else:
        _logger.info('training skipped, saving error tag')

    from plugins.result_storage.result_storage_plugin import ResultStorageHook
    ResultStorageHook.save_final_state(
        entity_id,
        final_state,
        error_tag=json.dumps(error_tags),
        training_dag_id=task_instance.dag_id,
        training_task_id=task_instance.task_id,
        training_execution_date=task_instance.execution_date
    )
    _logger.info('publishing result')
    trigger_push_result_to_mq(
        'final_result',
        final_state,
        entity_id,
        verify_error=result.get('verify_error'),
        curve_mode=result.get('error_tag')
    )
    _logger.info('all done.')


dag = DAG(
    dag_id=DAG_ID,
    description=u'上汽拧紧曲线训练任务',
    schedule_interval=schedule_interval,
    default_args=desoutter_default_args,
    max_active_runs=MAX_ACTIVE_TRAINING)

store_task = PythonOperator(provide_context=True,
                            task_id=TASK_ID, dag=dag,
                            python_callable=trigger_training_task)

# test
# https://airflow.apache.org/docs/apache-airflow/1.10.12/executor/debug.html
if __name__ == '__main__':
    dag.clear(reset_dag_runs=True)
    conf = {
        'entity_id': '3002/0000002062/1627459189',
        'final_state': 'OK',
        'error_tags': []
    }
    dag.run(conf=conf)
