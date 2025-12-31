with open('airflow-core/src/airflow/models/pool.py', 'r') as f:
    content = f.read()

# Fix the mixed quote on line 50
content = content.replace(
    'if not re.match(r"^[a-zA-Z0-9_.-]+$\', name):',
    'if not re.match(r"^[a-zA-Z0-9_.-]+$", name):'
)

with open('airflow-core/src/airflow/models/pool.py', 'w') as f:
    f.write(content)

print("Fixed!")
