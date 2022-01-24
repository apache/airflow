printf 'This will cause a coding error \xb1\xa6\x01\n' # v2.2.3: airflow/hooks/subprocess.py:88: UnicodeDecodeError: 'utf-8' codec can't decode byte 0xb1 in position 31: invalid start byte
