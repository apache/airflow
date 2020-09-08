#!/usr/bin/env bash
source ~/wockspace/venv/airflow37/bin/activate
/bin/bash ./airflow/compile_translation.sh
/bin/bash ./airflow/www_rbac/compile_assets.sh
pip install setuptools wheel gitpython
python ./setup.py sdist bdist_wheel
