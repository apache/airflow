#!/usr/bin/env bash
source ~/wockspace/venv/airflow37/bin/activate
pip install setuptools wheel gitpython
python ./setup.py sdist bdist_wheel
