FROM lyft/pythonlibrary:fd03a394897fe765bfd4a917192c500515922581
RUN mkdir /code/incubator-airflow
RUN cp /code/containers/pythonlibrary/Makefile /code/incubator-airflow/Makefile
COPY ops /code/incubator-airflow/ops
COPY . /code/incubator-airflow
