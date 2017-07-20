FROM lyft/pythonlibrary:fd03a394897fe765bfd4a917192c500515922581
RUN mkdir /code/incubator-airflow
RUN cp /code/containers/pythonlibrary/Makefile /code/incubator-airflow/Makefile
COPY ops /code/incubator-airflow/ops
COPY requirements.* piptools_requirements.* /code/lyftairflow/
RUN pip install -r /code/incubator-airflow/piptools_requirements.txt
RUN pip install -r /code/incubator-airflow/requirements.txt
COPY . /code/incubator-airflow
