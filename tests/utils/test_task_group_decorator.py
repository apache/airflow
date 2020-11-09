import pendulum

from airflow.models.dag import DAG
from airflow.operators.python import task, PythonOperator
from airflow.utils.task_group import taskgroup
from airflow.www.views import task_group_to_dict


def test_build_task_group_context_manager():
    """
    Tests Following :
    1. Nested TaskGroup creation using taskgroup decorator should create same TaskGroup which can be created using
    TaskGroup context manager.
    2. TaskGroup consisting Tasks created using task decorator.
    3. Node Ids of dags created with taskgroup decorator.
    """

    # Creating Tasks
    @task()
    def task_start():
        """Dummy Task which is First Task of Dag """
        return '[Task_start]'

    @task()
    def task_end():
        """Dummy Task which is Last Task of Dag"""
        print(f'[ Task_End ]')

    @task
    def task_1(value):
        """ Dummy Task1"""
        return f'[ Task1 {value} ]'

    @task
    def task_2(value):
        """ Dummy Task2"""
        print(f'[ Task2 {value} ]')

    @task
    def task_3(value):
        """ Dummy Task3"""
        return f'[ Task3 {value} ]'

    @task
    def task_4(value):
        """ Dummy Task3"""
        print(f'[ Task4 {value} ]')

    # Creating TaskGroups
    @taskgroup(group_id='section_1')
    def section_1(value):
        """ TaskGroup for grouping related Tasks"""

        @taskgroup(group_id='section_2')
        def section_2(value2):
            """ TaskGroup for grouping related Tasks"""
            return task_4(task_3(value2))

        op1 = task_2(task_1(value))
        return section_2(op1)

    execution_date = pendulum.parse("20201109")
    with DAG(dag_id="example_nested_task_group_decorator", start_date=execution_date, tags=["example"]) as dag:
        t1 = task_start()
        s1 = section_1(t1)
        s1.set_downstream(task_end())

    # Testing TaskGroup created using taskgroup decorator
    assert set(dag.task_group.children.keys()) == {"task_start", "task_end", "section_1"}
    assert set(dag.task_group.children['section_1'].children.keys()) == \
           {'section_1.task_1', 'section_1.task_2', 'section_1.section_2'}

    # Testing TaskGroup consisting Tasks created using task decorator
    assert dag.task_dict['task_start'].downstream_task_ids == {'section_1.task_1'}
    assert dag.task_dict['section_1.task_2'].downstream_task_ids == {'section_1.section_2.task_3'}
    assert dag.task_dict['section_1.section_2.task_4'].downstream_task_ids == {'task_end'}

    def extract_node_id(node, include_label=False):
        ret = {"id": node["id"]}
        if include_label:
            ret["label"] = node["value"]["label"]
        if "children" in node:
            children = []
            for child in node["children"]:
                children.append(extract_node_id(child, include_label=include_label))

            ret["children"] = children
        return ret

    # Node IDs test
    node_ids = {
        'id': None,
        'children':
            [{'id': 'section_1',
              'children': [{
                  'id': 'section_1.section_2',
                  'children': [
                      {'id': 'section_1.section_2.task_3'},
                      {'id': 'section_1.section_2.task_4'}]},

                  {'id': 'section_1.task_1'},
                  {'id': 'section_1.task_2'},
                  {'id': 'section_1.downstream_join_id'}]},

             {'id': 'task_end'},
             {'id': 'task_start'}]}

    assert extract_node_id(task_group_to_dict(dag.task_group)) == node_ids


def test_build_task_group_with_operators():
    """  Tests DAG with Tasks created with *Operators and TaskGroup created with taskgroup decorator """

    def task_start():
        """Dummy Task which is First Task of Dag """
        return '[Task_start]'

    def task_end():
        """Dummy Task which is Last Task of Dag"""
        print(f'[ Task_End  ]')

    # Creating Tasks
    @task
    def task_1(value):
        """ Dummy Task1"""
        return f'[ Task1 {value} ]'

    @task
    def task_2(value):
        """ Dummy Task2"""
        return f'[ Task2 {value} ]'

    @task
    def task_3(value):
        """ Dummy Task3"""
        print(f'[ Task3 {value} ]')

    # Creating TaskGroups
    @taskgroup(group_id='section_1')
    def section_1(value):
        """ TaskGroup for grouping related Tasks"""
        return task_3(task_2(task_1(value)))

    execution_date = pendulum.parse("20201109")
    with DAG(dag_id="example_task_group_decorator_mix", start_date=execution_date, tags=["example"]) as dag:
        t_start = PythonOperator(task_id='task_start', python_callable=task_start, dag=dag)
        s1 = section_1(t_start.output)
        t_end = PythonOperator(task_id='task_end', python_callable=task_end, dag=dag)
        s1.set_downstream(t_end)

    # Testing Tasks ing DAG
    assert set(dag.task_group.children.keys()) == {'section_1', 'task_start', 'task_end'}
    assert set(dag.task_group.children['section_1'].children.keys()) == \
           {'section_1.task_2', 'section_1.task_3', 'section_1.task_1'}

    # Testing Tasks downstream
    assert dag.task_dict['task_start'].downstream_task_ids == {'section_1.task_1'}
    assert dag.task_dict['section_1.task_3'].downstream_task_ids == {'task_end'}
