.PHONY: clean test run lint stop compile

test:
	echo "##### Running Tests #####"
	python -m  unittest discover . "test_*.py"

lint:
	autopep8 --in-place --aggressive --recursive --exclude="*.yml,Dockerfile,*.txt" . && yapf -i -r -vv .

run:
	docker build -t airflow_coatue .
	docker-compose -f docker-compose.yml up -d
	rm airflow_logs.log || echo "creating log file..." && docker-compose -f docker-compose.yml logs --no-color --tail=1000 --follow > airflow_logs.log &

stop:
	docker-compose -f docker-compose.yml down

compile:
	find . -type f -name '*.py' | xargs -n1 python -m py_compile
