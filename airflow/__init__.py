__version__ = "0.4.6"

'''
Authentication is implemented using flask_login and different environments can
implement their own login mechanisms by providing an `airflow_login` module
in their PYTHONPATH. airflow_login should be based off the
`airflow.www.login`
'''
try:
    # Environment specific login
    import airflow_login
    login = airflow_login
except ImportError:
    # Default login, no real authentication
    from airflow import default_login
    login = default_login

from models import DAG
