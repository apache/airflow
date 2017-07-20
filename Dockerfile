FROM lyft/pythonlibrary:fd03a394897fe765bfd4a917192c500515922581
RUN mkdir /code/lyftairflow
RUN cp /code/containers/pythonlibrary/Makefile /code/lyftairflow/Makefile
COPY ops /code/lyftairflow/ops
COPY requirements.* piptools_requirements.* /code/lyftairflow/
RUN pip install -r /code/lyftairflow/requirements.txt
WORKDIR /code/lyftairflow
COPY . /code/lyftairflow
