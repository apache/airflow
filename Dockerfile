FROM lyft/opsbase:294cbad348ad4db579087e22f65386ca6762ebfe
ARG IAM_ROLE
RUN mkdir /code/incubator-airflow
RUN cp /code/containers/pythonlibrary/Makefile /code/incubator-airflow/Makefile
COPY ops /code/incubator-airflow/ops
COPY requirements.* /code/incubator-airflow/
RUN pip install -r /code/incubator-airflow/requirements.txt
WORKDIR /code/incubator-airflow
COPY . /code/incubator-airflow
