content = open('task-sdk/src/airflow/sdk/bases/decorator.py', encoding='utf-8').read()
result = content.replace('import warnings\n', '')
open('task-sdk/src/airflow/sdk/bases/decorator.py', 'w', encoding='utf-8', newline='\n').write(result)
print('Done')
