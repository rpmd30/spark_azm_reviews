FROM jupyter/minimal-notebook

COPY ./requirements/requirements_jupyter.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt
