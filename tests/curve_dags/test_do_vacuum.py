# -*- coding:utf-8 -*-

import requests

body = {
    "replace_microseconds": 'false',
    "conf": {
        "delta_time": '1'
    }
}

import json

if __name__ == '__main__':
    data = json.dumps(body)
    result = requests.post('http://localhost:8080/api/experimental/dags/data_retention_policy/dag_runs', data=data)
    print(result.status_code)
