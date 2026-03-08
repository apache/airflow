content = open('task-sdk/src/airflow/sdk/bases/decorator.py', encoding='utf-8').read()
old = '        except NameError as e:\n            warnings.warn(\n                f"Cannot infer multiple_outputs for TaskFlow function {self.function.__name__!r} with forward"\n                f" type references that are not imported. (Error was {e})",\n                stacklevel=4,\n            )\n            return False'
new = '        except NameError:\n            # Forward references using TYPE_CHECKING-only imports are valid Python patterns.\n            # We cannot infer multiple_outputs when the type is not available at runtime.\n            return False'
assert old in content, 'Pattern not found!'
result = content.replace(old, new)
open('task-sdk/src/airflow/sdk/bases/decorator.py', 'w', encoding='utf-8', newline='\n').write(result)
print('Done')
